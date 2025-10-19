import websockets
import json
import requests
import asyncio
import os
import logging

from typing import List, Dict, Iterable, Set
from services.monitor import Monitor
from services.constants import STOP_COMMAND
from utils.enums import Currency, InstrumentType, Subscription

logger = logging.getLogger(__name__)

class InstrumentManager:
    def __init__(
        self,
        currencies: List[Currency],
        instrument: InstrumentType,
        data_dir: str,
        max_instr_per_monitor: int = 450,
    ):
        self.rest_url = "https://www.deribit.com/api/v2/public"
        self.ws_url = "wss://www.deribit.com/ws/api/v2/"

        self.currencies = currencies
        self.instrument = instrument
        self.data_dir = os.path.join(data_dir, instrument.value)

        self.max_instr_per_monitor = max_instr_per_monitor
        self._monitor_instances: Dict[str, Monitor] = {}
        self._active_monitor_tasks: Dict[str, asyncio.Task] = {}
        self._active_instruments: Dict[str, Set] = {}
        self._monitor_seq: Dict[str, int] = {
            currency.value:0 for currency in self.currencies
        }

    def _next_monitor_id(self, currency: str) -> str:
        self._monitor_seq[currency] += 1
        sequence_num = self._monitor_seq[currency]
        return f"{currency}-{self.instrument.value}-{sequence_num}"

    def _start_monitor(
        self,
        instruments: List[str],
        currency: str,
        monitor_id: str,
    ):
        new_monitor_instance = Monitor(
            instruments=instruments,
            output_dir=os.path.join(self.data_dir, currency),
        )

        self._monitor_instances[monitor_id] = new_monitor_instance

        new_monitor_task = new_monitor_instance.start_execution()
        self._active_monitor_tasks[monitor_id] = new_monitor_task

        logger.info(f"Monitor instance {monitor_id} instantiated.")

    async def _stop_monitor(
        self,
        monitor_id: str,
    ):
        if monitor_id in self._monitor_instances:
            monitor = self._monitor_instances[monitor_id]
            task = self._active_monitor_tasks[monitor_id]

            await monitor.query_queue.put(STOP_COMMAND)

            if task:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                    logger.info(f"Monitor {monitor_id} gracefully exited")
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Monitor {monitor_id} failed to gracefully exit. " +
                        "Cancelling task..."
                    )
                    task.cancel()
            del self._monitor_instances[monitor_id]

    def _get_instrument_query(
        self,
        instrument: str,
        type: Subscription,
        msg_id: int = 1,
        group: str = "none",
        depth: str = "20",
        interval: str = "100ms",
    ):
        if type == Subscription.SUBSCRIBE:
            method = "public/subscribe"
        elif type == Subscription.UNSUBSCRIBE:
            method = "public/unsubscribe"

        channels = [
           f"book.{instrument}.{group}.{depth}.{interval}"
        ]

        msg = {
            "jsonrpc": "2.0",
            "id": msg_id, 
            "method": method, 
            "params": {
                "channels": channels
            },
        }
        return json.dumps(msg)

    async def _start_monitor_single(
        self,
        new_instrument: str,
        currency: str,
    ) -> str:
        for monitor_id, monitored_insturments in \
            self._active_instruments.items():
            if (currency not in monitor_id or 
                len(monitored_insturments) >= self.max_instr_per_monitor):
                continue

            monitored_insturments.add(new_instrument)
            target_monitor = self._monitor_instances[monitor_id]

            query = self._get_instrument_query(
                instrument=new_instrument,
                type=Subscription.SUBSCRIBE,
            )

            await target_monitor.query_queue.put_nowait(query)
            return monitor_id

        # If all monitors are full, we create a new one
        monitor_id = self._next_monitor_id(currency)
        self._start_monitor(
            instruments=[new_instrument],
            currency=currency,
            monitor_id=monitor_id,
        )
        self._active_instruments[monitor_id] = {new_instrument}
        return monitor_id

    # def _add_instrument(
    #     self,
    #     instrument_name,
    # ):
    #     pass

    def _get_metadata(self) -> Dict:
        """
        Retrieves the latest instrument metadata for the currencies being
        managed
        """
        result = {}

        for currency in self.currencies:
            params = {
                "currency":currency.value,
                "kind":self.instrument.value,
                "expired":"false",
            }
            response = requests.get(
                f"{self.rest_url}/get_instruments",
                params=params
            )
            data = response.json()

            if data.get("result"):
                metadata = data["result"]
                result[currency.value] = metadata

        return result

    def _startup(self) -> None:
        """
        Initializes monitoring for active instruments across supported
        currencies.
        
        This method retrieves metadata for all supported currencies, batches the
        list of active instruments for each currency (with a maximum batch size
        specified by Deribit API's websocket connection limits), and starts a
        monitor instance for each batch.
        
        The batching ensures that no websocket connection exceeds the allowed
        number of instruments per connection.
        """
        def batch(instruments, b_size) -> Iterable[List[Dict]]:
            for i in range(0, len(instruments), b_size):
                yield instruments[i:i + b_size]

        # Query current active instruments for the different currencies
        currency_metadata = self._get_metadata()

        for currency, active_instruments in currency_metadata.items():
            # Deribit API says each websocket connection supports up to 500
            # instruments at a time
            batches = batch(active_instruments, self.max_instr_per_monitor)

            for instrument_batch in batches:
                instruments = [
                    instr["instrument_name"] for instr in instrument_batch
                ]
                monitor_id = self._next_monitor_id(currency)
                self._start_monitor(
                    instruments=instruments,
                    currency=currency,
                    monitor_id=monitor_id,
                )
                self._active_instruments[monitor_id] = set(instruments)

    async def run(self):
        """
        We need to query the REST endpoint for the current list of active
        instruments first. Then we instantiate all the monitor instances.

        Then start async.run() on the instrument status websocket monitor
        """
        self._startup()

        async with websockets.connect(self.ws_url) as websocket:
            channels = [
               f"instrument.state.{currency.value}.{self.instrument.value}"
               for currency in self.currencies
            ]
            msg = {
                "jsonrpc": "2.0",
                "method": "public/subscribe",
                "id": 0,
                "params": {
                   "channels": channels
                }
            }
            await websocket.send(json.dumps(msg))
            ack = await websocket.recv()

            while True:
                response = await websocket.recv()
                data = response.json()["data"]

                state = data["state"]
                instrument_name = data["instrument_name"]

                match state:
                    case "started" | "created":
                       logger.info(
                           f"Initiation signal {state} "
                           f"recieved for {instrument_name}"
                       )

                    case "terminated" | "settled" | "closed":
                        logger.info(
                            f"Stopping signal {state} "
                            f"recieved for {instrument_name}"
                        ) 
                    case "deactivated":
                        logger.info(
                            f"Deactivation signal {state} "
                            f"recieved for {instrument_name}"
                        )
                    case _:
                        logger.error(
                            f"Unknown state {state} enocuntered"
                        )
