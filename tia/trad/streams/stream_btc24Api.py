import time
import tia.trad.market.m as markets; reload(markets)
import tia.trad.tools.classOps as classes; reload(classes)
from tia.trad.tools.errf import eReport
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.net.httpApi as httpApi; reload(httpApi)
import tia.trad.tools.ipc.zmqHelper as zmqHelper
import sys
import zmq
import tia.trad.tools.ipc.processLogger as pl
import logging

LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
API_LIMIT = 10 * 10 ** 6    # in usec since comparing to .lastUpdate
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
        elif _mode == names.onTicker:
            eventMsg = _container
        elif _mode == names.onLag:
            eventMsg = {"name": names.onLag, "market": marketName, "date": date, "lag": _container}
        else: raise Exception("unknown mode")

        _QMainStream.send_json(eventMsg)
    except Exception: raise


def getNewTrades(_Market):
    try:
        logger.debug("getNewTrades: hi")

        newTrades_ = []
        tradesD = httpApi.getTrades(_Market)

        if tradesD:
            lTid = _Market.lastTid
            if lTid:
                # just search for new ones
                for tid in tradesD:
                    if tid > lTid:
                        price = tradesD[tid]["price"]
                        amount = tradesD[tid]["amount"]
                        date = tradesD[tid]["date"]
                        newTrades_.append({"tid": tid, "price": price, "amount": amount, "date": date})
                        lTid = tid
                _Market.lastTid = lTid

            else:
                # just append the last one
                latestTid = tradesD.largest_key()
                price = tradesD[latestTid]["price"]
                amount = tradesD[latestTid]["amount"]
                date = tradesD[latestTid]["date"]
                newTrades_.append({"tid": latestTid, "price": price, "amount": amount, "date": date})
                _Market.lastTid = latestTid
        return newTrades_
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
        # init subclasses
        for marketName in marketObjD:
            marketObjD[marketName] = marketObjD[marketName]()
            marketObjD[marketName].changed = 1  # so that we get the orderbook first
            marketObjD[marketName].gotFirstOrdb = 0
            marketObjD[marketName].prevOrderbook = None
        return marketObjD
    except Exception: raise

def run(**kwargs):
    try:
        global logger
        global ZMQPRODUCER
        logger = kwargs["processLogger"]
        httpApi.logger = logger
        logger.debug("run: hi")

        ZMQPRODUCER = zmqHelper.getProducer(ZMQCONTEXT)

        marketObjD = config()
        Market = marketObjD["btc24EUR"]
        now = 10**16

        while 1:
            # periodically send the whole orderbook
            orderbook = httpApi.getOrderbook(Market)
            if orderbook:
                pushToStream(names.onOrderbook, orderbook, Market, ZMQPRODUCER)
            time.sleep(1)
            newTrades = getNewTrades(Market)
            pushToStream(names.onBatchTrades, newTrades, Market, ZMQPRODUCER)

    except Exception as ex:
        print "%s:ex: %s" % (__file__, ex)
        eReport(__file__)
        sys.exit()



