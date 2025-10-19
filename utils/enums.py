from enum import Enum, auto

class Currency(Enum):
    BTC = "BTC"
    ETH = "ETH"

class InstrumentType(Enum):
    OPTION = "option"

class Subscription(Enum):
    SUBSCRIBE = auto()
    UNSUBSCRIBE = auto()