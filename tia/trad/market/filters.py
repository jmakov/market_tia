import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class Filter(object):
    def __repr__(self): return "%s(%s)" % (self.__class__.__name__, self.__dict__)
    def __init__(self):
        self._softFilters = 0
        self._hardFilters = 0
        self.passed = None

        # if any of these 0 -> passed = 0|1
        self.minAskChanged = 1     # onDelta
        self.maxBidChanged = 1     # onDelta
        self.secondaryAskChanged = 1
        self.secondaryBidChanged = 1

        # if any of these 0 -> passed = 0
        #onOrderbook
        self.feedNotAnomalous = 1
        self.noLag = 1
        self.spreadNotAnomalous = 1
        self.gotOrderbook = 0   # don't pass until all got their orderbook
        self.liveTrade = 1
        #synch msgs
        self.synchedWithExchange = 1
        #onTrade
        self.dontWaitForTransactionList = 1
        # wait till all markets get orderbook
    def __setattr__(self, key, value):
        try:
            self.__dict__[key] = value

            if key in ["minAskChanged", "maxBidChanged", "secondaryAskChanged", "secondaryBidChanged"]:
                self._softFilters = any([self.minAskChanged, self.maxBidChanged, self.secondaryAskChanged, self.secondaryBidChanged])
                self.passed = all([self._softFilters, self._hardFilters])
            elif key in ["gotOrderbook", "liveTrade", "feedNotAnomalous", "noLag", "spreadNotAnomalous", "synchedWithExchange", "dontWaitForTransactionList"]:
                self._hardFilters = all([self.gotOrderbook, self.liveTrade, self.feedNotAnomalous, self.noLag, self.spreadNotAnomalous, self.synchedWithExchange, self.dontWaitForTransactionList])
                self.passed = all([self._softFilters, self._hardFilters])
        except AttributeError: pass   # raises at init only
        except Exception: raise



def setFilterTopOrders(_oldTopList, _newTopList, _Market):
    try:
        _Market.filters.maxBidChanged = 1 if _oldTopList[0] != _newTopList[0] else 0
        _Market.filters.secondaryBidChanged = 1 if _oldTopList[1] != _newTopList[1] else 0
        _Market.filters.minAskChanged = 1 if _oldTopList[2] != _newTopList[2] else 0
        _Market.filters.secondaryAskChanged = 1 if _oldTopList[3] != _newTopList[3] else 0
    except Exception: raise
def getTopOrders(_Market):
    try:
        iterBid = _Market.orderbook.bids.iterkeys(reverse=True)
        l1bid = iterBid.next()
        l2bid = iterBid.next()

        iterAsk = _Market.orderbook.asks.iterkeys()
        l1ask = iterAsk.next()
        l2ask = iterAsk.next()

        return [l1bid, l2bid, l1ask, l2ask]
    except StopIteration: return [0, 0, 0, 0]  # can happen that a market doesn't have the orderbook yet
    except Exception: raise

"""
integrity checks
"""
def checkSpread(_Market):
    try:
        anomalous = 0

        minAsk = _Market.orderbook.asks.smallest_key()
        maxBid = _Market.orderbook.bids.largest_key()
        if minAsk < maxBid:
            anomalous = True

        _Market.filters.spreadNotAnomalous = 0 if anomalous else 1
    except KeyError: _Market.filters.spreadNotAnomalous = 0
    except Exception: raise
def checkOrderbook(_Market):
    try:
        anomalous = False
        # check for negative values
        index = 0
        for item in _Market.orderbook.bids.iteritems(reverse=True):
            price = item[0]
            amount = item[1]
            if price < 0:
                anomalous = True
                break
            if amount < 0:
                anomalous = True
                break
            index += 1
            if index > 10: break
        index = 0
        for item in _Market.orderbook.asks.iteritems():
            price = item[0]
            amount = item[1]
            if price < 0:
                anomalous = True
                break
            if amount < 0:
                anomalous = True
                break
            index += 1
            if index > 10: break

        _Market.filters.feedNotAnomalous = 0 if anomalous else 1

        # check that minask > maxbid
        checkSpread(_Market)
    except Exception: raise


