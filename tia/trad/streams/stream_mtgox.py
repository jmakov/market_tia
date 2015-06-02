import sys
import json
from tia.trad.tools.errf import eReport
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.ipc.zmqHelper as zmqHelper
from tia.trad.tools.timing import getTime
from ws4py.client.geventclient import WebSocketClient
import gevent.event
import gevent_zeromq as zmq
#import tia.trad.tools.net.socketio_mtgox as io
import logging
import tia.trad.tools.ipc.processLogger as pl

LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
ZMQPRODUCER = None
ZMQCONTEXT = zmq.Context()


class MyWS(WebSocketClient):
    def __init__(self, _MainStream, _logger, _url):
        try:
            WebSocketClient.__init__(self, _url)
            self.shutdown_cond = gevent.event.Event()
            self.mylogger = _logger
            self.MainStream = _MainStream
            self.url = _url
        except Exception: raise

    def closed(self, code, reason): self.shutdown_cond.set()

    def connectAndWait(self):
        try:
            self.mylogger.debug("connecting")
            self.connect()
            self.shutdown_cond.wait(timeout=60)
        except Exception: raise

    def received_message(self, _msg):
        try:
            self.mylogger.debug("received: %s" % _msg)

            wsMsg = _msg.data
            jsonObj = json.loads(wsMsg, parse_float=str)

            trade =  "dbf1dee9-4f2e-4a08-8cb7-748919a71b21" #Trades
            ticker = "d5f06780-30a8-4a48-a2f8-7ed181b4a13f" #Ticker USD            print "hi"

            depth =  "24e67e0d-1cad-4cc0-9e7a-f8523ef460fe" #Depth  USD

            channel = jsonObj['channel']
            if channel==trade:    channel_name = 'trade'
            elif channel==ticker: channel_name = 'ticker'
            elif channel==depth:  channel_name = 'depth'
            else:                 channel_name = 'unknown'

            eventMsg = None
            op = jsonObj['op'] #'private'
            if op == 'subscribe':
                sys.stdout.write("subscribed to channel %s\n" % channel_name); sys.stdout.flush()
            elif op == 'unsubscribe':
                sys.stdout.write(jsonObj); sys.stdout.flush()
            elif op == 'remark':
                sys.stdout.write(jsonObj); sys.stdout.flush()
            elif op == 'private':
                #origin = jsonObj['origin'] #'broadcast'
                private = jsonObj['private'] #ticker, trade, depth

                if private=='trade' and channel==trade:
                    tradeData = jsonObj["trade"]
                    if tradeData["price_currency"] == "USD" and tradeData["primary"] == "Y":
                        date = tradeData["date"]
                        price = tradeData["price"]
                        amount = tradeData["amount"]
                        tid = tradeData["tid"]
                        eventMsg = {"name": names.onTrade, "market": "mtgoxUSD", "date": date, "price": price, "amount": amount, "tid": tid}
                elif private=='depth' and channel==depth:
                    depthData = jsonObj["depth"]
                    if depthData["currency"] == "USD":
                        amount = str(fl.D(depthData["total_volume_int"]) / 10**8)
                        side = depthData["type_str"] + "s"
                        eventMsg = {"name": names.onDelta, "market": "mtgoxUSD", "date": depthData["now"], "price": depthData["price"], "amount": amount, names.deltaSide: side}
                elif private=='ticker' and channel==ticker:
                    tickerData = jsonObj["ticker"]
                    if tickerData["avg"]["currency"] == "USD":
                        minAsk = tickerData["sell"]["value"]
                        maxBid = tickerData["buy"]["value"]
                        eventMsg = {"name": names.onTicker, "market": "mtgoxUSD", "minAsk": minAsk, "maxBid": maxBid, "date": getTime()}

            if eventMsg: self.MainStream.send_json(eventMsg)
        except Exception: raise

def callback(msg):
    print msg

def shutdown():
    try:
        ZMQPRODUCER.close()
        ZMQCONTEXT.term()
    except Exception: raise
def run(**kwargs):
    try:
        global logger
        global ZMQPRODUCER
        logger = kwargs["processLogger"]
        logger.debug("%s: hi" % __file__)
        print "%s: connecting" % __file__

        ZMQPRODUCER = zmqHelper.getProducer(ZMQCONTEXT)
        # connect to websocket
        url = "ws://websocket.mtgox.com:80/mtgox"
        #socketioUrl = "socketio.mtgox.com:80/mtgox"
        #sio = io.SocketIO(socketioUrl, callback)
        #sio.connect()

        while 1:
            try:
                myClient = MyWS(ZMQPRODUCER, logger, url)
                myClient.connectAndWait()
            except Exception:
                myClient.mylogger.info("reconnecting")
                myClient.shutdown_cond.set()
                myClient.close_connection()
                continue

        logger.debug("%s: bye" % __file__)
    except Exception as ex:
        print "%s:ex: %s" % (__file__, ex)
        eReport(__file__)
        sys.exit()



if __name__ == "__main__":
    run(None, None)
