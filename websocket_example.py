import asyncio
import websockets
import json

from pprint import pprint 

BASE_URL = "https://www.deribit.com/api/v2/public"
WS_URL = "wss://www.deribit.com/ws/api/v2/"
# WS_URL = "wss://test.deribit.com/ws/api/v2/"

msg = {
    "id":8772,
    "jsonrpc":"2.0",
    "method":"public/get_order_book",
    "params":{"depth":5,"instrument_name":"BTC-9OCT25-123000-P"}
}

# Example for subscribing to order book updates
msg = {
    "jsonrpc": "2.0",
    "id": 42,
    "method": "public/subscribe",
    "params": {
        "channels": [
            "book.BTC-31OCT25-116000-C.none.20.100ms",
            # book.{instrument_name}.{group}.{depth}.{interval}
        ]
    }
}

# Example for subscribing to instrument updates
msg = {
    "jsonrpc": "2.0",
    "method": "public/subscribe",
    "id": 42,
    "params": {
       "channels": [
           "instrument.state.any.any",
           # instrument.state.{kind}.{currency}
           # instrument.state.option.BTC
        ]
    }
}

# Example for getting order book for a bitcoin instrument
# msg = {
#     "jsonrpc": "2.0",
#     "id": 42,
#     "method": "public/get_order_book",
#     "params": {
#         "depth":10,
#         "instrument_name":"BTC-10OCT25-121000-C",
#     }
# }

def design_api_call(
    instruments,
):
    channels = [
        f"book.{instrument}.none.20.100ms" for instrument in instruments
    ]

    msg = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "public/subscribe",
        "params": {
            "channels": channels
        }
    }

    return msg

async def call_api(msg):
    async with websockets.connect(WS_URL) as websocket:
        await websocket.send(msg)
        try:
            while True:
                response = await websocket.recv()
                pprint(json.loads(response))
        except websockets.ConnectionClosed:
            print("Connection closed.")

asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))