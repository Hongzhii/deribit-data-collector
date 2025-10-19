import asyncio
import json
from pathlib import Path

import pytest
import websockets

from services.monitor import Monitor


def test_instruments_to_channels_builds_expected_topics(tmp_path):
    monitor = Monitor(instruments={"BTC-1"}, output_dir=str(tmp_path))
    assert monitor.channels == ["book.BTC-1.none.20.100ms"]


def test_get_query_returns_valid_json(tmp_path):
    monitor = Monitor(instruments={"BTC-1"}, output_dir=str(tmp_path))
    payload = monitor._get_query(["book.BTC-1.none.20.100ms"], "SUBSCRIBE", id=7)
    data = json.loads(payload)
    assert data["method"] == "public/subscribe"
    assert data["id"] == 7
    assert data["params"]["channels"] == ["book.BTC-1.none.20.100ms"]


@pytest.mark.asyncio
async def test_flush_buffer_writes_to_disk(tmp_path):
    monitor = Monitor(instruments={"BTC-1"}, output_dir=str(tmp_path))
    update = {
        "instrument_name": "BTC-1",
        "timestamp": 123,
        "bids": [],
        "asks": [],
    }

    monitor.buffer["BTC-1"].append(update)
    monitor.cur_buffer_size = 1

    monitor.flush_buffer()

    # Wait for background write to finish
    if monitor.active_tasks:
        await asyncio.gather(*monitor.active_tasks)

    output_file = Path(tmp_path) / "BTC-1.txt"
    assert output_file.exists()
    contents = output_file.read_text().strip()
    assert "timestamp" in contents
    assert monitor.cur_buffer_size == 0
    assert monitor.buffer["BTC-1"] == []


@pytest.mark.asyncio
async def test_process_external_queries_sends_items(tmp_path):
    monitor = Monitor(instruments={"BTC-1"}, output_dir=str(tmp_path))

    class DummyWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, msg):
            self.sent.append(msg)

        async def recv(self):
            return json.dumps({"jsonrpc": "2.0", "result": "ok"})

    dummy_ws = DummyWebSocket()
    task = asyncio.create_task(monitor._process_external_queries(dummy_ws))

    await monitor.query_queue.put("message")
    await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert dummy_ws.sent == ["message"]





@pytest.mark.asyncio
async def test_monitor_attempts_flush_on_fatal_error(monkeypatch, tmp_path):
    monitor = Monitor(instruments={"BTC-1"}, output_dir=str(tmp_path))
    monitor.cur_buffer_size = 1

    flushed = False

    def fake_flush():
        nonlocal flushed
        flushed = True

    monitor.flush_buffer = fake_flush  # type: ignore[assignment]

    class DummyWebSocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send(self, msg):
            return None

        async def recv(self):
            return json.dumps({"jsonrpc": "2.0", "result": "ok"})

    def fake_connect(*args, **kwargs):
        return DummyWebSocket()

    async def crash_monitor(self, websocket):
        raise RuntimeError("boom")

    async def noop_process(self, websocket):
        await asyncio.sleep(0)

    monkeypatch.setattr(websockets, "connect", fake_connect)
    monkeypatch.setattr(Monitor, "_monitor", crash_monitor)
    monkeypatch.setattr(Monitor, "_process_external_queries", noop_process)

    await monitor.run()

    assert flushed
