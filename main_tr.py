import tia.trad.tools.ipc.processLogger as pl
pl.PROCESS_NAME = "main_tr."
import zmq
import multiprocessing
import sys
import tia.configuration as conf
from tia.trad.market.events import handledEventsD
import tia.trad.market.orderManager as orderManager; reload(orderManager)
import tia.trad.tools.classOps as classes
import tia.trad.market.m
import tia.trad.market.router as router; reload(router)
import tia.trad.market.strat as strat; reload(strat)
from tia.trad.tools.errf import eReport
from tia.trad.tools.ipc.processLogger import loggerInit
import tia.trad.tools.ipc.process as process; reload(process)
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.ipc.zmqHelper as zmqHelper
import tia.trad.market.events as events; reload(events)
import json
import tia.trad.tools.arithm.floatArithm as fl

logger = None



def saveTM(_Market):
    try:
        data = {"targetFunds": {}, "targetItems": {}}
        for pr in _Market.targetManager.targetFunds:
            data["targetFunds"][str(pr)] = str(_Market.targetManager.targetFunds[pr])
        for pr in _Market.targetManager.targetItems:
            data["targetItems"][str(pr)] = str(_Market.targetManager.targetItems[pr])

        with open(conf.FN_TM, "w") as f:
            json.dump(data, f)
    except Exception: raise
def loadTM(_Market):
    try:
        with open(conf.FN_TM, "r") as f:
            data = json.load(f)

        for pr in data["targetFunds"]:
            _Market.targetManager.targetFunds[fl.D(pr)] = fl.D(data["targetFunds"][pr])
        for pr in data["targetItems"]:
            _Market.targetManager.targetItems[fl.D(pr)] = fl.D(data["targetItems"][pr])
    except ValueError: pass     #if nothing in file
    except Exception: raise

def getConfig(_action, argD):
    try:
        global logger
        # get logger
        logger = loggerInit(conf.DIR_LOG + "main_tr")

        jobs = []
        # get market classes
        marketsD = {}
        subclassesD = classes.getSubclasses(tia.trad.market.m)
        for marketName in subclassesD:
            # and init them
            marketsD[marketName] = subclassesD[marketName]()
            # and select strategy
            marketsD[marketName].activeStrategies.registerStrategy(strat.MarketMaker)


        if _action == "sim":
            import tia.trad.sim as sim
            import tia.trad.tools.arithm.floatArithm as fl
            # overwrite events with sim structures
            handledEventsD[names.onTrade] = sim.onTrade
            handledEventsD[names.onOrderbook] = sim.onOrderbook
            # set init funds
            for Market in marketsD.values():
                if conf.UNIVERSE in Market.name:
                    exchangeRate = Market.exchangeRates[Market.currency]
                    initFunds = str(argD["initFunds"])
                    Market.account.setBalance(str(0), fl.D(str(argD["initItems"])), str(0), initFunds)
            # set mock functions/classes
            zmqContext = None
            MainStream = sim.MainStream(conf.FN_DB_SIM)
            RecordStream = sim.RecordStream()
            router.synchronizeWithExchanges = sim.synchronizeWithExchanges
            # touch a new report file
            f = open(conf.FN_REPORT, "w"); f.close()

            # start monitoring the report file
            monitor = multiprocessing.Process(target=process.Process, args=("tia.trad.monitor_mainTr",),
                                             kwargs=({"initFunds": argD["initFunds"], "initItems": argD["initItems"]})); jobs.append(monitor)
        else:
            if _action == "record":
                import tia.trad.sim as sim
                for Market in marketsD.values(): Market.filters.liveTrade = 0
                handledEventsD[names.onBatchTrades] = sim.onBatchTradesMock
            elif _action == "live":
                #get balance for markets, open orders etc.
                btc24 = marketsD["btc24EUR"]
                loadTM(btc24)
                events.getAccStatus(btc24)

                btc24.router = multiprocessing.Queue()
                syncherBtc24 = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.syncher",), kwargs=({"Queue": marketsD["btc24EUR"].router})); jobs.append(syncherBtc24)
                monitor = multiprocessing.Process(target=process.Process, args=("tia.trad.monitor_mainTr",),
                                                  kwargs=({"initFunds": 1, "initItems": 1})); jobs.append(monitor)
            else: raise Exception("unhandled action: %s" % _action)

            # setup IPC messaging
            zmqContext = zmq.Context()
            MainStream = zmqHelper.getConsumerServer(zmqContext)
            RecordStream = zmqHelper.getRecorderServer(zmqContext)
            # define processes
            streamApi = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.stream_apis", )); jobs.append(streamApi)
            #streamIntrsng = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.stream_intrsng",)); jobs.append(streamIntrsng)
            #streamMtGox = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.stream_mtgox",)); jobs.append(streamMtGox)
            streamMtgoxApi = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.stream_mtgoxAPI",)); jobs.append(streamMtgoxApi)
            streamBtc24Api = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.stream_btc24Api",)); jobs.append(streamBtc24Api)
            recorder = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.recorder",)); jobs.append(recorder)

        # start processes
        for job in jobs: job.start()

        return [zmqContext, logger, handledEventsD, marketsD, MainStream, RecordStream, jobs]
    except Exception: raise


if __name__ == "__main__":
    try:
        argD = {}
        action = sys.argv[1]
        zmqContext = MainStream = RecordStream = None
        # check that requested action is supported
        if action not in conf.AVAILABLE_ACTIONS: raise Exception("action not in %s" % conf.AVAILABLE_ACTIONS)
        if action == "sim":
            argD["initFunds"] = float(sys.argv[2])
            argD["initItems"] = float(sys.argv[3])

        # prepare datajobs
        [zmqContext, logger, eventD, marketsD, MainStream, RecordStream, jobs] = getConfig(action, argD)


        # runtime
        logger.debug("runtime: hi")
        while 1:
            eventMsg = MainStream.recv_json()
            Event = handledEventsD[eventMsg["name"]](**eventMsg)

            RecordStream.send_json(eventMsg)

            # update configuration
            Market = marketsD[Event.market]

            if conf.UNIVERSE in Market.name:
                logger.debug("event: %s" % Event.name)
                Event.handle(marketsD)

                if Market.filters.passed:
                    orderManager.manageUniverse(Event.date, Market, marketsD)
                # since some events require direct access to the router, they set filters to passed=False ad go straight to the router
                router.synchronizeWithExchanges(marketsD)

    except KeyboardInterrupt:
        sys.stderr.write("\n%s: received KeyboardInterrupt. Passing it to all subprocesses.\n" % (__file__))
        saveTM(Market)
        zmqHelper.closeZmq(zmqContext, MainStream, RecordStream)
        for job in jobs: job.join()
        sys.exit()
    except Exception as ex:
        zmqHelper.closeZmq(zmqContext, MainStream, RecordStream)
        eReport(__file__)
        sys.exit()
