import time
import ujson as json
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.market.events as events
import logging
import tia.trad.tools.io.follow as io
import tia.trad.market.filters as filters
from tia.trad.tools.ipc.processLogger import PROCESS_NAME

LOGGER_NAME = PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class MainStream(object):
    def __init__(self, _dbPath):
        try:
            self.dbPath = _dbPath
            self.file = open(self.dbPath, "r")
            self.lineIterator = io.followSim(self.file)
        except Exception: raise
    def recv_json(self):
        try:
            line = self.lineIterator.next()
            dct = json.loads(line)
            return dct
        except StopIteration:
            print "\nFIN: no more data"
            time.sleep(100000)
        except ValueError:
            print "\nFIN: no more data"
            time.sleep(100000)
        except Exception: raise

class RecordStream(object):
    def send_json(self, _dummyEvent): pass

def synchronizeWithExchanges(_MarketsD):
    # simulate instant exchange response
    try:
        logger.debug("synchronizeWithExchanges: hi")
        for Market in _MarketsD.values():
            while 1:
                try:
                    Event = Market.ordersQueue.popleft()
                    if Event.name == names.orderPlace:
                        Market.activeOrders.place(Event.order)
                        Market.orderbook.simPlace(Event.order)
                    elif Event.name == names.orderCancel:
                        Market.activeOrders.cancel(Event.order)
                        Market.orderbook.simCancel(Event.order)
                    else: raise Exception("unknown name: %s" % Event.name)
                except IndexError: break

            Market.filters.synchedWithExchange = 1  #net: will actually be an Event notifying the market it's in sync
    except Exception: raise

class onTrade(events.onTrade):
    # 1. get executedAmount for targetManager.updateExposure. If amount, run .updateExposure
    # 2. get new configuration of AO
    # 3. account for Strat
    def handle(self, _MarketsD):
        try:
            logger.debug("onTrade:handle: hi")

            Market = _MarketsD[self.market]
            if Market.filters.passed:
                # since we don't have real time streams for most of the exchanges
                # most of the trades are in or out of spread. so estimate the side
                dta = abs(self.price - Market.orderbook.asks.smallest_key())
                dtb = abs(self.price - Market.orderbook.bids.largest_key())

                executedSide = names.asks if dta < dtb else names.bids
                if executedSide == names.asks:
                    if Market.activeOrders.asks:
                        #if self.price >= Market.activeOrders.asks.price:
                        # update AO amount
                        ActiveOrder = Market.activeOrders.asks
                        executedAmount = min(self.amount, ActiveOrder.amount)
                        Market.activeOrders.tradedAmount(executedAmount, ActiveOrder, _MarketsD)
                        # update TM
                        Market.targetManager.update(executedAmount, ActiveOrder.price, ActiveOrder.type, Market)
                        #Market.targetManager.updateExposure(self.date, names.asks, _MarketsD)
                elif executedSide == names.bids:
                    if Market.activeOrders.bids:
                        #if self.price <= Market.activeOrders.bids.price:
                        ActiveOrder = Market.activeOrders.bids
                        executedAmount = min(self.amount, ActiveOrder.amount)
                        Market.activeOrders.tradedAmount(executedAmount, ActiveOrder, _MarketsD)
                        # update TM
                        Market.targetManager.update(executedAmount, ActiveOrder.price, ActiveOrder.type, Market)
                        #Market.targetManager.updateExposure(self.date, names.asks, _MarketsD)
        except Exception: raise
class onOrderbook(events.onOrderbook):
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date

            oldTopList = filters.getTopOrders(Market)

            # update ordb
            Market.orderbook = self.orderbook
            #simulate our orders
            if Market.activeOrders.asks:
                aoPrice = Market.activeOrders.asks.price
                aoAmount = Market.activeOrders.asks.amount
                Market.orderbook.asks[aoPrice] = aoAmount
            if Market.activeOrders.bids:
                aoPrice = Market.activeOrders.bids.price
                aoAmount = Market.activeOrders.bids.amount
                Market.orderbook.bids[aoPrice] = aoAmount

            newTopList = filters.getTopOrders(Market)

            # integrity check
            filters.checkOrderbook(Market)

            # set filters
            filters.setFilterTopOrders(oldTopList, newTopList, Market)

            Market.filters.gotOrderbook = 1
        except StopIteration: Market.orderbook = self.orderbook
        except Exception: raise
class onBatchTradesMock(events.onBatchTrades):
    def handle(self, _MarketsD):
        try: pass
        except Exception: raise