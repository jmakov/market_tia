import time
import socket
import json
import sys
from tia.trad.tools.errf import eReport
from tia.trad.tools.timing import getTime
import tia.trad.market.m as m
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.ipc.zmqHelper as zmqHelper
import zmq
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)

BUFFER_SIZE = 8192 * 2
SOCK = None
ZMQPRODUCER = None
ZMQCONTEXT = zmq.Context()


def callback(_msg, _MainStream):
    try:
        msgType = _msg[0]
        dataD = _msg[1]
        eventMsg = None
        # fill IPC containers
        if msgType == "orderbook": pass
        elif msgType == "depth":
            try:
                marketName = m.Intersango.idNameMap[dataD["currency_pair_id"]]
                price = dataD["rate"]
                amount = dataD["amount"]
                side = dataD["type"]
                date = getTime()
                eventMsg = {"name": names.onDelta, "market": marketName, "date": date, "price": price, "amount": amount, names.deltaSide: side}
            except Exception: raise
        elif msgType == "trade":
            try:
                marketName = m.Intersango.idNameMap[dataD["currency_pair_id"]]
                price = dataD["rate"]
                amount = dataD["amount"]
                tid = dataD["trade_id"]
                date = getTime()
                eventMsg = {"name": names.onTrade, "market": marketName, "date": date, "price": price, "amount": amount, "tid": tid}
            except Exception: raise
        elif msgType == "ping": pass
        else: print "unknown msg", _msg

        if eventMsg: _MainStream.send_json(eventMsg)
    except Exception: raise


def reconnect():
    try:
        logger.info("reconnecting: hi")
        global SOCK
        SOCK.shutdown(2)
        SOCK = socket.create_connection(('db.intersango.com', 1337))
        SOCK.settimeout(60)
    except Exception:
        time.sleep(30)
        reconnect()
def connect():
    try:
        logger.info("connecting: hi")
        global SOCK
        SOCK = socket.create_connection(('db.intersango.com', 1337))
        SOCK.settimeout(60)
    except Exception: raise

def shutdown():
    try:
        SOCK.close()
        ZMQPRODUCER.close()
        ZMQCONTEXT.term()
    except Exception: raise
def run(**kwargs):
    try:
        #global logger
        global ZMQPRODUCER
        #logger = kwargs["processLogger"]
        logger.info("%s: hi" % __file__)

        ZMQPRODUCER = zmqHelper.getProducer(ZMQCONTEXT)
        connect()
        print "%s: connecting" % __file__

        buff= ''
        while 1:
            try:
                buff += SOCK.recv(BUFFER_SIZE)
                logger.debug("buffer: %s" % buff)

                if not buff:
                    logger.info("buffer empty")
                    reconnect()

                # reading buffer
                while '\r\n' in buff:
                    index = buff.find('\r\n')
                    msg = buff[:index].strip()
                    buff = buff[index+2:]
                    msg = json.loads(msg)
                    # processing data
                    callback(msg, ZMQPRODUCER)
            except socket.timeout:
                logger.error("socket.timeout")
                reconnect()
                continue
            except Exception: raise
    except Exception as ex:
        print "%s:ex: %s" % (__file__, ex)
        eReport(__file__)
        sys.exit()