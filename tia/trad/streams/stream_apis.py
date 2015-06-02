import tia.trad.tools.dicDiff as diff
import tia.trad.market.m as markets; reload(markets)
import tia.trad.tools.classOps as classes
from tia.trad.tools.errf import eReport
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.net.httpApi as httpApi; reload(httpApi)
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.tools.ipc.zmqHelper as zmqHelper
import sys
import tia.trad.tools.timing as timing
import zmq

logger = None
API_LIMIT = 10 * 10**6     # usec
ORDERBOOK_RESOLUTION = 60 * 10**6
ZMQPRODUCER = None
ZMQCONTEXT = zmq.Context()


def pushToStream(_mode, _container, _Market, _QMainStream):
    try:
        logger.debug("pushToStream: hi: %s" % _mode)

        marketName = _Market.name
        date = _Market.lastUpdate

        if _mode == names.onOrderbook:
            eventMsg = {"name": names.onOrderbook, "market": marketName, "date": date, "orderbook": _container}
        elif _mode == names.onBatchTrades:
            eventMsg = {"name": names.onBatchTrades, "market": marketName, "date": date, names.container: _container}
        elif _mode == names.onBatchDeltas:
            eventMsg = {"name": names.onBatchDeltas, "market": marketName, "date": date, "container": _container}
        else: raise Exception("unknown mode")

        _QMainStream.send_json(eventMsg)
    except Exception: raise


def getNewTrades(_Market):
    try:
        logger.debug("getNewTrades: hi")

        marketName = _Market.name
        newTrades_ = []


        lTid = _Market.lastTid
        if lTid:
            since = ""
            if "btc24" in _Market.name: since = str(lTid)
            tradesD = httpApi.getTrades(_Market, since)
            # just search for new ones
            if tradesD:
                for tid in tradesD:
                    if tid > lTid:
                        price = tradesD[tid]["price"]
                        amount = tradesD[tid]["amount"]
                        date = tradesD[tid]["date"]
                        newTrades_.append({"tid": tid, "price": price, "amount": amount, "date": date})
                        lTid = tid
                _Market.lastTid = lTid

        else:
            tradesD = httpApi.getTrades(_Market)
            # just append the last one
            if tradesD:
                latestTid = tradesD.largest_key()
                price = tradesD[latestTid]["price"]
                amount = tradesD[latestTid]["amount"]
                date = tradesD[latestTid]["date"]
                newTrades_.append({"tid": latestTid, "price": price, "amount": amount, "date": date})
                _Market.lastTid = latestTid
        return newTrades_
    except Exception: raise

def getDeltas(_currOrdb, _prevOrdb):
    try:
        logger.debug("getDeltas: hi")

        container_ = []

        diffD = diff.orderbookComparison(_currOrdb, _prevOrdb)

        for side in [names.bids, names.asks]:
            for delta in diffD[side]:
                changedPrices = diffD[side][delta]
                for price in changedPrices:
                    if delta == names.added:
                        amount = diffD[side]["curr"][price]
                        deltaD = {"price": price, "amount": amount, names.deltaSide: side}
                        container_.append(deltaD)
                    elif delta == names.removed:
                        amount = "-" + diffD[side]["prev"][price] #_currOrdb[side][price]
                        deltaD = {"price": price, "amount": amount, names.deltaSide: side}
                        container_.append(deltaD)
                    elif delta == names.changed:
                        amount =  str(fl.D(diffD[side]["curr"][price]) - fl.D(diffD[side]["prev"][price])) #str(fl.D(_currOrdb[side][price]) - fl.D(_prevOrdb[side][price]))
                        deltaD = {"price": price, "amount": amount, names.deltaSide: side}
                        container_.append(deltaD)

        logger.debug("deltas: %s" % container_)
        return container_ if container_ else None
    except Exception: raise

def shutdown():
    try:
        ZMQPRODUCER.close()
        ZMQCONTEXT.term()
    except Exception: raise
def config():
    try:
        # get subclasses
        marketObjD = classes.getSubclasses(markets)

        marketsD = {}
        # init subclasses
        for marketName in marketObjD:
            if marketName not in ["mtgoxUSD, btc24EUR"]:
                marketsD[marketName] = marketObjD[marketName]()
                marketsD[marketName].changed = 1  # so that we get the orderbook first
                marketsD[marketName].gotFirstOrdb = 0
                marketsD[marketName].prevOrderbook = None

        return marketsD
    except Exception: raise

def run(**kwargs):
    try:
        global logger
        global ZMQPRODUCER
        logger = kwargs["processLogger"]
        logger.debug("run: hi")

        ZMQPRODUCER = zmqHelper.getProducer(ZMQCONTEXT)

        marketObjD = config()
        now = 10**16

        while 1:
            # get new depth
            for marketName in marketObjD:
                Market = marketObjD[marketName]

                # periodically send the whole orderbook
                if now - Market.lastUpdate > API_LIMIT:
                    orderbook = httpApi.getOrderbook(Market)
                    if orderbook:
                        pushToStream(names.onOrderbook, orderbook, Market, ZMQPRODUCER)
                        """
                        if Market.gotFirstOrdb:     # for calc deltas
                            batchDeltas = getDeltas(orderbook, Market.prevOrderbook)
                            if batchDeltas:
                                pushToStream(names.onBatchDeltas, batchDeltas, Market, producer)
                                Market.changed = 1
                                Market.prevOrderbook = orderbook
                        # checkpoint orderbook
                        if now - Market.lastUpdate > ORDERBOOK_RESOLUTION:
                            # send only if market changed
                            if Market.changed:
                                pushToStream(names.onOrderbook, orderbook, Market, producer)
                                Market.changed = 0
                                Market.gotFirstOrdb = 1
                                Market.prevOrderbook = orderbook
                        """
                    # get trades for non streaming markets
                    newTrades = getNewTrades(Market)
                    if newTrades: pushToStream(names.onBatchTrades, newTrades, Market, ZMQPRODUCER)

            now = timing.getTime()
    except Exception as ex:
        print "%s:ex: %s" % (__file__, ex)
        eReport(__file__)
        sys.exit()



