import websockets
import logging
import json
import copy
import os
import asyncio
from collections import defaultdict
from typing import List, Mapping, Optional, Set

from services.constants import STOP_COMMAND

logger = logging.getLogger(__name__)

class Monitor:
    def __init__(
        self,
        instruments: Set[str],
        output_dir: str,
        group: Optional[str]="none",
        depth: Optional[str]="20",
        interval: Optional[str]="100ms",
        max_buffer_size: int=1000,
        reconnect_delay: float=5.0,
    ):
        self.group = group
        self.depth = depth
        self.interval = interval

        self.channels = self._instruments_to_channels(instruments)
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.rest_url = "https://www.deribit.com/api/v2/public"
        self.ws_url = "wss://www.deribit.com/ws/api/v2/"

        self.buffer = defaultdict(list)
        self.max_buffer_size = max_buffer_size
        self.cur_buffer_size = 0

        self.reconnect_delay = reconnect_delay
        self.query_queue = asyncio.Queue()

        # For ensurring that buffer writes complete during shutdown
        self.active_tasks: Set[asyncio.Task] = set()

    def _instruments_to_channels(
        self,
        instruments,
    ) -> List[str]:
        channels = []

        for instrument in instruments:
            channel = (
                f"book.{instrument}.{self.group}."
                f"{self.depth}.{self.interval}"
            )
            channels.append(channel)

        return channels
    
    def _get_query(
        self,
        channels,
        type: str,
        id: int = 0,
    ) -> str:
        if type == "SUBSCRIBE":
            method = "public/subscribe"
        elif type == "UNSUBSCRIBE":
            method = "public/unsubscribe"

        msg = {
            "jsonrpc": "2.0",
            "id": id, 
            "method": method, 
            "params": {
                "channels": channels
            },
        }
        return json.dumps(msg)


    async def _process_external_queries(
        self,
        websocket
    ):
        try:
            while True:
                query = await self.query_queue.get()

                if query == STOP_COMMAND:
                    logger.debug(
                        "Stop command recieved from instrument manager"
                    )
                    self.flush_buffer()
                    raise asyncio.CancelledError()

                await websocket.send(query)
                ack = await websocket.recv()

        except asyncio.CancelledError:
            logger.debug(
                "Asyncio cancelled signal recieved in _process_external_queries"
            )
            raise

    async def _prepare_graceful_shutdown(
        self
    ):
        self.flush_buffer()

        if self.active_tasks:
            await asyncio.gather(*self.active_tasks)

    async def _monitor(
        self,
        websocket,
    ):
        try:
            while True:
                response = await websocket.recv()
                data = json.loads(response)["params"]["data"]

                self.buffer[data["instrument_name"]].append(data)
                self.cur_buffer_size += 1

                if self.cur_buffer_size > self.max_buffer_size:
                    self.flush_buffer()
        except asyncio.CancelledError:
            logger.debug(
                "Asyncio cancelled signal received in _monitor"
            )
            raise
        except Exception as e:
            logger.fatal(
                f"Unexpected exception {e} occured in _monitor"
            )
            raise

    def flush_buffer(self):
        """
        Makes a deep copy of the buffer and writes it to file. Ensures that I/O
        does not block websocket monitoring.
        """
        logger.info(f"Flushing {self.cur_buffer_size} items from buffer...")
        buffer = copy.deepcopy(self.buffer)
        task = asyncio.create_task(
            asyncio.to_thread(
                self.write_buffer,
                buffer,
            )
        )
        task.add_done_callback(self.active_tasks.discard)
        self.active_tasks.add(task)

        
        # Reset buffer and count
        self.buffer = defaultdict(list)
        self.cur_buffer_size = 0

    def compress(self, update):
        result = {}

        result["timestamp"] = update["timestamp"]
        result["bids"] = update["bids"]
        result["asks"] = update["asks"]

        return str(result)

    def write_buffer(self, buffer):
        """
        We only need to keep the essential informaiton,
        design a process_information method to do this.

        We need:
            - timestamp
            - Bid/ask (this can be optimized further for redundanat info)
            - change_id (ensures data integrity)
        """
        try:
            for instrument in buffer:
                with open(
                    os.path.join(self.output_dir, instrument) + ".txt", "a"
                ) as f:
                    for update in buffer[instrument]:
                        f.write(self.compress(update) + "\n")
        except Exception as e:
            logger.fatal(f"Error writing buffer to disk: {e}")

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    initial_query = self._get_query(
                        self.channels,
                        "SUBSCRIBE",
                    )

                    await websocket.send(initial_query)
                    ack = await websocket.recv()

                    await asyncio.gather(
                        self._monitor(websocket),
                        self._process_external_queries(websocket),
                    )
            except (asyncio.exceptions.CancelledError, KeyboardInterrupt):
                await self._prepare_graceful_shutdown()
                logger.info("Shutdown complete, exiting...")
                break
            except websockets.ConnectionClosed as e:
                self.flush_buffer()
                logger.error(f"Connection error encountered: {e}")
                logger.info(f"Attempt reconnect after {self.reconnect_delay}s")
                await asyncio.sleep(self.reconnect_delay)
                continue
            except Exception as e:
                await self._prepare_graceful_shutdown()
                break

    def start_execution(self):
        return asyncio.create_task(self.run())


