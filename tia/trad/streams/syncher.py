import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.market.m as m; reload(m)
from tia.trad.tools.errf import eReport
import tia.trad.tools.ipc.zmqHelper as zmqHelper
import tia.trad.tools.net.httpApi as httpApi; reload(httpApi)
import zmq
import sys
import json
import logging
import tia.trad.tools.ipc.processLogger as pl
import tia.configuration as conf
import Queue

LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
MARKET = None
ZMQPRODUCER = None
ZMQCONTEXT = zmq.Context()


def getCredentialsDict(_filename):
    try:
        # TODO decryption
        with open(_filename, "r") as f:
            credentialsD = json.load(f)
        return credentialsD
    except Exception: raise
def loadCredentials(_Market):
    # load credentials for API
    try:
        credentialsD = getCredentialsDict(conf.FN_CREDENTIALS)
        exchangeName = _Market.name[:-3]
        _Market.account.setUserCredentials(exchangeName, credentialsD)
    except Exception: raise

def loadLastTid(_Market):
    try:
        with open(conf.FN_LTID, "r") as f:
            _Market.lastTid = json.load(f)["lastTid"]
    except ValueError:
        _Market.lastTid = 0
    except Exception: raise
def saveLastTid(_Market):
    try:
        with open(conf.FN_LTID, "w") as f:
            data = {"lastTid": _Market.lastTid}
            json.dump(data, f)
    except Exception: raise



def prepareMarketState(_Market):
    try:
        loadCredentials(_Market)
        loadLastTid(_Market)
    except Exception: raise
def exitMarket(_Market):
    try:
        for AO in [_Market.activeOrders.asks, _Market.activeOrders.bids]:
            if AO:
                httpApi.cancelOrder(_Market, AO.oid, AO.type)
    except Exception: raise


def handleEvent(_Market, _Event):
    try:
        logger.debug("received: %s" % _Event)
        # sync until answ
        zmqMsg = _Event.sync(_Market)
        if zmqMsg:
            logger.debug("sent: %s" % zmqMsg)
            ZMQPRODUCER.send_json(zmqMsg)
    except Exception: raise
def shutdown():
    try:
        saveLastTid(MARKET)
        #exitMarket(MARKET)
        ZMQPRODUCER.close()
        ZMQCONTEXT.term()
    except Exception: raise
def run(**kwargs):
    try:
        global ZMQPRODUCER
        global MARKET
        global logger
        ZMQPRODUCER = zmqHelper.getProducer(ZMQCONTEXT)
        logger = kwargs["processLogger"]
        logger.debug("%s: hi" % __file__)
        IpcQueue = kwargs["Queue"]

        Market = m.btc24EUR()
        MARKET = Market
        prepareMarketState(Market)


        while 1:
            Event = IpcQueue.get(block=True)
            handleEvent(Market, Event)
            while 1:
                try:
                    Event = IpcQueue.get(block=False)
                    handleEvent(Market, Event)
                except Queue.Empty:
                    synchedMsg = {"name": names.onSynchedWithExchange, "market": Market.name}
                    ZMQPRODUCER.send_json(synchedMsg)
                    logger.debug("sent: %s\n" % synchedMsg)
                    break

    except Exception as ex:
        print "%s:ex: %s" % (__file__, ex)
        eReport(__file__)
        sys.exit()

