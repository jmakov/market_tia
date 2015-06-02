import logging
import tia.trad.tools.timing as tm
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.market.filters as filters; reload(filters)
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.market.orderbook as ordb; reload(ordb)
import tia.trad.market.orders as order; reload(order)
import tia.trad.tools.net.httpApi as httpApi; reload(httpApi)
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


"""
events that update the orderbook
"""
class onDelta(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, price, amount, side):
        try:
            self.name = names.onDelta
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.price = fl.D(price)
            self.amount = fl.D(amount)
            self.type = side
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date
            decisionD = {names.bids: Market.orderbook.bids, names.asks: Market.orderbook.asks}
            orderbook = decisionD[self.type]

            oldTopList = filters.getTopOrders(Market)

            # upd orderbook
            if "mtgox" in Market.name:
                orderbook[self.price] = self.amount
            else:
                try: orderbook[self.price] += self.amount
                except KeyError: orderbook[self.price] = self.amount
            if orderbook[self.price] <= 0: del orderbook[self.price]

            newTopList = filters.getTopOrders(Market)

            # set filters
            filters.setFilterTopOrders(oldTopList, newTopList, Market)

            # integrity check
            filters.checkSpread(Market)
        except KeyError: pass
        except Exception: raise


class onBatchDeltas(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, container):
        try:
            self.name = names.onBatchDeltas
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.container = []
            for dct in container:
                dct["name"] = names.onDelta
                dct["market"] = self.market
                dct["date"] = self.date
                self.container.append(onDelta(**dct))
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date
            decisionD = {names.bids: Market.orderbook.bids, names.asks: Market.orderbook.asks}

            oldMinAsk = Market.orderbook.asks.smallest_key()
            oldMaxBid = Market.orderbook.bids.largest_key()

            # upd orderbook
            for Delta in self.container:
                orderbook = decisionD[Delta.type]
                try: orderbook[Delta.price] += Delta.amount
                except KeyError: orderbook[Delta.price] = Delta.amount
                if orderbook[Delta.price] <= 0: del orderbook[Delta.price]

            # integrity cehck
            filters.checkOrderbook(Market)

            # set filters
            Market.filters.minAskChanged = 1 if oldMinAsk != Market.orderbook.asks.smallest_key() else 0
            Market.filters.maxBidChanged = 1 if oldMaxBid != Market.orderbook.bids.largest_key() else 0
        except KeyError: pass
        except Exception: raise


class onOrderbook(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, orderbook):
        try:
            self.name = names.onOrderbook
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.orderbook = ordb.SortedOrderbook(orderbook)
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date

            oldTopList = filters.getTopOrders(Market)

            # update ordb
            Market.orderbook = self.orderbook
            newTopList = filters.getTopOrders(Market)

            # integrity check
            filters.checkOrderbook(Market)

            # set filters
            filters.setFilterTopOrders(oldTopList, newTopList, Market)

            # always manage universe
            Market.filters.gotOrderbook = 1
        except StopIteration: Market.orderbook = self.orderbook
        except Exception: raise

class onTicker(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, minAsk, maxBid):
        try:
            self.name = names.onTicker
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.minAsk = fl.D(minAsk)
            self.maxBid = fl.D(maxBid)
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date
            if Market.orderbook.bids.largest_key() > self.maxBid:
                for bid in Market.orderbook.bids.iterkeys(reverse=True):
                    if bid > self.maxBid:
                        del Market.orderbook.bids[bid]
                    else: break
            if Market.orderbook.asks.smallest_key() < self.minAsk:
                for ask in Market.orderbook.asks.iterkeys():
                    if ask < self.minAsk:
                        del Market.orderbook.asks[ask]
                    else: break
            Market.orderbook.bids[self.maxBid] = fl.D("1")
            Market.orderbook.asks[self.minAsk] = fl.D("1")
            # integrity cehck
            filters.checkOrderbook(Market)
        except KeyError as ex:
            logger.exception("onTicker:AttrErr: %s" % ex)
            Market.orderbook.bids[self.maxBid] = fl.D("1")
            Market.orderbook.asks[self.minAsk] = fl.D("1")
        except Exception: raise

class onLag(object):
    def __init__(self, name, market, date, lag):
        try:
            self.name = names.onLag
            assert name == self.name, (self.name, name)
            self.market = market
            self.date = date
            self.lag = lag
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.lastUpdate = self.date
            Market.filters.noLag = 1 #if self.lag < 30000000 else 0  # order lag in usec
        except Exception: raise
"""
events that update active orders
"""
def getAObyOID(_Market, _oid):
    try:
        res_ = None
        AOB = _Market.activeOrders.bids
        AOA = _Market.activeOrders.asks

        if AOB:
            if AOB.oid == _oid: res_ = [_Market.activeOrders.bids, names.bids]
        if AOA:
            if AOA.oid == _oid: res_ = [_Market.activeOrders.asks, names.asks]
        return res_
    except Exception: raise


class onTrade(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, price, amount, tid):
        try:
            self.name = names.onTrade
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.price = fl.D(price)
            self.amount = fl.D(amount)
            self.tid = tid
        except Exception: raise
    def handle(self, _MarketsD):
        try: pass
        except Exception: raise

class onBatchTrades(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, date, container):
        try:
            self.name = names.onBatchTrades
            assert self.name == name, (self.name, name)
            self.market = market
            self.date = date
            self.container = []
            for dct in container:
                dct["name"] = names.onTrade
                dct["market"] = self.market
                self.container.append(onTrade(**dct))
        except Exception: raise
    def handle(self, _MarketsD):
        # request transaction list to see if anything of ours partially executed
        try:
            Market = _MarketsD[self.market]

            # get avail and reserved funds
            #Market.ordersQueue.append(onAccountBalance(names.onAccountBallance, Market.name))  # confuses local state
            Market.ordersQueue.append(onOpenOrders(names.onOpenOrders, Market.name))

            # skip universe management and get new universe configuration
            Market.filters.synchedWithExchange = 0
        except Exception: raise


"""
events sent to syncProcesses
"""
class onPlaceOrder(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, orderPrice, orderAmount, orderType, eventDate=None, foundTargets=None, oid=None):
        try:
            self.name = names.orderPlace
            assert self.name == name, (self.name, name)
            assert orderAmount >= fl.D("0.09"), (orderPrice, orderAmount)
            self.market = market
            self.order = order.Order(market, orderPrice, orderAmount, orderType, eventDate, foundTargets, oid)
            self.date = tm.getTime()
        except Exception: raise
    def sync(self, _Market):
        try:
            msg_ = httpApi.placeOrder(_Market, self.order)
            # append targets
            if names.accountNotEnoughFunds in msg_:
                msg_ = httpApi.getAccountBalance(_Market)
                #msg_ = httpApi.getOpenOrders(_Market)
                #print "onPlaceOrder:notEnough:funds: %s, \nOrder: %s" % (_Market.account, self.order)

            return msg_
        except Exception: raise
    def handle(self, _MarketsD):
        # place AO and update Account
        try:
            Market = _MarketsD[self.order.market]
            Market.activeOrders.place(self.order)
        except Exception: raise

class onCancelOrder(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, oid, side, order=None):
        try:
            assert side in [names.asks, names.bids], side
            assert type(oid) == int, oid
            self.name = names.orderCancel
            assert self.name == name, (self.name, name)
            self.market = market
            self.oid = int(oid)
            self.type = side
            self.date = tm.getTime()
            self.order = order if order else None
        except Exception: raise
    def sync(self, _Market):
        try:
            msg_ = httpApi.cancelOrder(_Market, self.oid, self.type)
            if names.noSuchOrder in msg_:
                msg_ = httpApi.getOpenOrders(_Market)
            return msg_
        except Exception: raise
    def handle(self, _MarketsD):
        # remove appropriate AO
        try:
            Market = _MarketsD[self.market]
            # find which one to cancel
            [AO, side]  = getAObyOID(Market, self.oid)
            if AO: Market.activeOrders.cancel(AO)
        except Exception: raise

class onTransactionsList(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, transactions=None):
        try:
            self.name = names.onTransactionsList
            assert self.name == name, (self.name, name)
            self.market = market
            self.transactions = transactions
            self.date = tm.getTime()
            raise # currently we dont use this stuff
        except Exception: raise
    def sync(self, _Market):
        # {name:, transactions:{}}
        try:
            msg_ = httpApi.getTransactions(_Market)
            return msg_
        except Exception: raise
    def handle(self, _MarketsD):
        try:

            # if exe, get exe amount, cancel opposite ao
            # get acc info -> avail
            # send M.synched=1 event
            Market = _MarketsD[self.market]

            Market.ordersQueue.append(onOpenOrders(names.onOpenOrders, Market.name))

            # skip universe management and get new universe configuration
            Market.filters.synchedWithExchange = 0
        except Exception: raise


class onAccountBalance(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, fundsAvailable = None, itemsAvailable = None):
        if fundsAvailable: verify = type(fundsAvailable); assert verify in [str, unicode], verify
        if itemsAvailable: verify = type(itemsAvailable); assert verify in [str, unicode], verify
        self.name = names.onAccountBallance
        assert self.name == name, (self.name, name)
        self.market = market
        self.fundsAvail = fundsAvailable
        self.itemsAvail = itemsAvailable
        self.date = tm.getTime()
    def sync(self, _Market):
        try:
            msg_ = httpApi.getAccountBalance(_Market)
            return msg_
        except Exception: raise
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.account.availableFunds = fl.D(self.fundsAvail)
            Market.account.availableItems = fl.D(self.itemsAvail)
        except Exception: raise

class onOpenOrders(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market, orders=None):
        try:
            self.name = names.onOpenOrders
            assert self.name == name, (self.name, name)
            self.market = market
            self.orders = orders    # [{},...]
            self.date = tm.getTime()
        except Exception: raise
    def sync(self, _Market):
        try:
            msg_ = httpApi.getOpenOrders(_Market)
            return msg_
        except Exception: raise

    def estimateSide(self, _Market, _orderPrice):
        try:
            dta = abs(_orderPrice - _Market.orderbook.asks.smallest_key())
            dtb = abs(_orderPrice - _Market.orderbook.bids.largest_key())

            executedSide = names.asks if dta < dtb else names.bids
            return executedSide
        except Exception: raise
    def getAllAOoids(self, _Market):
        try:
            AOB = _Market.activeOrders.bids
            AOA = _Market.activeOrders.asks
            allOids = []
            if AOB: allOids.append(AOB.oid)
            if AOA: allOids.append(AOA.oid)
            return allOids
        except Exception: raise

    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            assert len(self.orders) <= 2, (self.orders, Market.activeOrders)

            allAOOids = self.getAllAOoids(Market)
            allOpenOrderOids = []
            # handle still open but maybe partially executed orders
            for dct in self.orders:
                oid = int(dct["oid"])
                allOpenOrderOids.append(oid)

                res = getAObyOID(Market, oid)
                if res:
                    [AO, side] = res
                else:
                    AO = None
                    side = self.estimateSide(Market, fl.D(dct["price"]))

                if AO:
                    exeAmount = AO.amount - fl.D(dct["amount"])
                    if exeAmount:
                        # update account
                        Market.activeOrders.tradedAmount(exeAmount, AO, _MarketsD)
                        # update TM:
                        Market.targetManager.update(exeAmount, AO.price, side, Market)
                else:
                    ao = order.Order(self.market, dct["price"], dct["amount"], side, dct["eventDate"], {"btc24EUR": None}, dct["oid"])
                    AO = ao

            # handle potentially wholly executed orders e.g. AO that are here but are not in self.orders since they were executed
            if allAOOids:
                for oid in allAOOids:
                    if oid not in allOpenOrderOids:
                        [AO, side]  = getAObyOID(Market, oid)
                        if AO:
                            exeAmount = AO.amount
                            # update account
                            Market.activeOrders.tradedAmount(exeAmount, AO, _MarketsD)
                            # update TM:
                            Market.targetManager.update(exeAmount, AO.price, side, Market)


        except Exception:  raise

class onSynchedWithExchange(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, name, market):
        self.name = names.onSynchedWithExchange
        assert self.name == name, (self.name, name)
        self.market = market
        self.date = tm.getTime()
    def handle(self, _MarketsD):
        try:
            Market = _MarketsD[self.market]
            Market.filters.synchedWithExchange = 1
        except Exception: raise


def getAccStatus(_Market):
    try:
        _Market.ordersQueue.append(onOpenOrders(names.onOpenOrders, _Market.name))
        _Market.ordersQueue.append(onAccountBalance(names.onAccountBallance, _Market.name))
        _Market.filters.synchedWithExchange = 0  # so that we skip universe management1000
    except Exception: raise

handledEventsD = {names.onDelta: onDelta,
                  names.onBatchDeltas: onBatchDeltas,
                  names.onOrderbook: onOrderbook,
                  names.onTrade: onTrade,
                  names.onBatchTrades: onBatchTrades,
                  names.onLag: onLag,
                  names.onTicker: onTicker,

                  names.orderPlace: onPlaceOrder,
                  names.orderCancel: onCancelOrder,
                  names.onAccountBallance: onAccountBalance,
                  names.onTransactionsList: onTransactionsList,
                  names.onOpenOrders: onOpenOrders,
                  names.onSynchedWithExchange: onSynchedWithExchange
                }
privateEventsD = {
                names.orderPlace: onPlaceOrder,
                names.orderCancel: onCancelOrder,
                names.onAccountBallance: onAccountBalance,
                names.onTransactionsList: onTransactionsList,
                names.onOpenOrders: onOpenOrders,
                names.onSynchedWithExchange: onSynchedWithExchange
                }