from tia.trad.tools.sortedDict import SortedDict
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class DataWindow(object):
    def __repr__(self): return "%s(%s)" % (self.__class__.__name__, self.__dict__)
    def __init__(self):
        self.lastSentCheckpoint = 0

        self.deltas = {}
        self.prevOrderbook = {}

        self.lastTid = 0
        self.newTrades = SortedDict()



"""
def pMinAsk(_market):
    try: return _market.orderbook.asks._orderbook.smallest_key()
    except Exception: raise
def pMaxBid(_market):
    try: return _market.orderbook.bids._orderbook.largest_key()
    except Exception: raise
def pSpread(_Market):
    try:
        res_ = pMinAsk(_Market) - pMaxBid(_Market)
        assert res_ >= 0, [res_, pMaxBid(_Market), pMinAsk(_Market)]
        return res_
    except Exception: raise
"""