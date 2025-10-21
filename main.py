import asyncio
import logging

from services.instrument_manager import InstrumentManager
from utils.enums import Currency, InstrumentType

logging.basicConfig(level=logging.INFO)

manager = InstrumentManager(
    currencies=[Currency.BTC, Currency.ETH],
    instrument=InstrumentType.OPTION,
    data_dir="./data",
)

asyncio.run(manager.run())