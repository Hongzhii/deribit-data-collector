import asyncio

from services.instrument_manager import InstrumentManager
from utils.enums import Currency, InstrumentType

manager = InstrumentManager(
    currencies=[Currency.BTC, Currency.ETH],
    instrument=InstrumentType.OPTION,
    data_dir="./data",
)

asyncio.run(manager.run())