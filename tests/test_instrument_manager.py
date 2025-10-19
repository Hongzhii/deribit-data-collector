import asyncio
import json
from types import SimpleNamespace

import pytest

from services.instrument_manager import InstrumentManager, STOP_COMMAND
from utils.enums import Currency, InstrumentType, Subscription


class DummyQueue:
    def __init__(self):
        self.items = []

    async def put(self, item):
        self.items.append(item)

    async def put_nowait(self, item):
        self.items.append(item)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def dummy_monitor_cls(event_loop):
    class DummyMonitor:
        def __init__(self, instruments, output_dir, **kwargs):
            self.instruments = set(instruments)
            self.output_dir = output_dir
            self.kwargs = kwargs
            self.query_queue = DummyQueue()
            self.start_calls = 0

        def start_execution(self):
            self.start_calls += 1
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = event_loop
            fut = loop.create_future()
            fut.set_result(None)
            return fut

    return DummyMonitor


@pytest.fixture
def instrument_manager(monkeypatch, tmp_path, dummy_monitor_cls):
    monkeypatch.setattr("services.instrument_manager.Monitor", dummy_monitor_cls)
    manager = InstrumentManager(
        currencies=[Currency.BTC],
        instrument=InstrumentType.OPTION,
        data_dir=str(tmp_path),
        max_instr_per_monitor=2,
    )
    return manager


def test_get_instrument_query_builds_subscribe_payload(instrument_manager):
    payload = instrument_manager._get_instrument_query(
        instrument="BTC-TEST", type=Subscription.SUBSCRIBE, msg_id=99
    )

    data = json.loads(payload)
    assert data["method"] == "public/subscribe"
    assert data["id"] == 99
    assert data["params"]["channels"] == ["book.BTC-TEST.none.20.100ms"]


def test_start_monitor_registers_monitor(monkeypatch, instrument_manager, tmp_path):
    monitor_id = instrument_manager._next_monitor_id("BTC")
    instrument_manager._start_monitor([
        "BTC-1",
        "BTC-2",
    ], "BTC", monitor_id)

    assert monitor_id in instrument_manager._monitor_instances
    task = instrument_manager._active_monitor_tasks[monitor_id]
    assert task.done()


@pytest.mark.asyncio
async def test_start_monitor_single_reuses_existing_monitor(instrument_manager, dummy_monitor_cls):
    monitor_id = "BTC-option-1"
    existing_monitor = dummy_monitor_cls({"BTC-OLD"}, instrument_manager.data_dir)
    instrument_manager._monitor_instances[monitor_id] = existing_monitor
    instrument_manager._active_instruments[monitor_id] = {"BTC-OLD"}

    result = await instrument_manager._start_monitor_single("BTC-NEW", "BTC")

    assert result == monitor_id
    assert "BTC-NEW" in instrument_manager._active_instruments[monitor_id]
    assert len(existing_monitor.query_queue.items) == 1
    request = json.loads(existing_monitor.query_queue.items[0])
    assert request["method"] == "public/subscribe"
    assert request["params"]["channels"] == ["book.BTC-NEW.none.20.100ms"]


@pytest.mark.asyncio
async def test_start_monitor_single_creates_new_monitor_when_all_full(instrument_manager):
    instrument_manager.max_instr_per_monitor = 1
    monitor_id = instrument_manager._next_monitor_id("BTC")

    class FullQueue:
        async def put(self, item):
            pass

        async def put_nowait(self, item):
            pass

    instrument_manager._monitor_instances[monitor_id] = SimpleNamespace(query_queue=FullQueue())
    instrument_manager._active_instruments[monitor_id] = {"BTC-1"}

    new_id = await instrument_manager._start_monitor_single("BTC-NEW", "BTC")

    assert new_id != monitor_id
    assert new_id in instrument_manager._monitor_instances
    assert instrument_manager._active_instruments[new_id] == {"BTC-NEW"}


def test_startup_batches_metadata(monkeypatch, instrument_manager):
    sample_metadata = {
        "BTC": [
            {"instrument_name": "BTC-1"},
            {"instrument_name": "BTC-2"},
            {"instrument_name": "BTC-3"},
        ]
    }

    monkeypatch.setattr(
        instrument_manager,
        "_get_metadata",
        lambda: sample_metadata,
    )

    instrument_manager.max_instr_per_monitor = 2
    instrument_manager._startup()

    assert len(instrument_manager._monitor_instances) == 2
    assert sum(len(v) for v in instrument_manager._active_instruments.values()) == 3


@pytest.mark.asyncio
async def test_stop_monitor_sends_stop_command(instrument_manager):
    monitor_id = "BTC-option-1"
    queue = DummyQueue()
    instrument_manager._monitor_instances[monitor_id] = SimpleNamespace(query_queue=queue)
    task = asyncio.create_task(asyncio.sleep(0))
    instrument_manager._active_monitor_tasks[monitor_id] = task

    await instrument_manager._stop_monitor(monitor_id)

    await asyncio.sleep(0)  # let callbacks process
    assert STOP_COMMAND in queue.items
    assert monitor_id not in instrument_manager._monitor_instances


@pytest.mark.asyncio
async def test_stop_monitor_cancels_hung_task_on_timeout(monkeypatch, instrument_manager):
    monitor_id = "BTC-option-1"
    queue = DummyQueue()
    instrument_manager._monitor_instances[monitor_id] = SimpleNamespace(query_queue=queue)

    async def never_finishes():
        await asyncio.Future()

    running_task = asyncio.create_task(never_finishes())
    instrument_manager._active_monitor_tasks[monitor_id] = running_task

    async def fake_wait_for(task, timeout):  # pragma: no cover - helper
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)

    await instrument_manager._stop_monitor(monitor_id)
    await asyncio.sleep(0)

    assert running_task.cancelled()
    assert STOP_COMMAND in queue.items
    assert monitor_id not in instrument_manager._monitor_instances


@pytest.mark.asyncio
async def test_stop_monitor_propagates_stop_to_monitor_instance(instrument_manager):
    monitor_id = instrument_manager._next_monitor_id("BTC")
    instrument_manager._start_monitor(["BTC-TEST"], "BTC", monitor_id)
    monitor = instrument_manager._monitor_instances[monitor_id]

    await instrument_manager._stop_monitor(monitor_id)

    assert STOP_COMMAND in monitor.query_queue.items
    assert monitor_id not in instrument_manager._monitor_instances