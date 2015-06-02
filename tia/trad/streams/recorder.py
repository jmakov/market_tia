import tia.configuration as conf
from tia.trad.tools.errf import eReport
import tia.trad.tools.ipc.zmqHelper as zmqHelper
from tia.trad.market.events import privateEventsD
import json
import sys
import zmq

DBFILE = open(conf.FN_DB, "a")
ZMQCONSUMER = None
ZMQCONTEXT = zmq.Context()

def shutdown():
    try:
        DBFILE.flush()
        DBFILE.close()
        ZMQCONSUMER.close()
        ZMQCONTEXT.term()
    except Exception: raise

def run(**kwargs):
    try:
        global ZMQCONSUMER
        processLogger = kwargs["processLogger"]
        processLogger.debug("recorder: hi")
        ZMQCONSUMER = zmqHelper.getRecorderConsumer(ZMQCONTEXT)

        while 1:
            eventMsg = ZMQCONSUMER.recv_json()
            if eventMsg["name"] in privateEventsD:
                continue
            json.dump(eventMsg, DBFILE)
            DBFILE.write("\n")

    except Exception:
        DBFILE.close()
        eReport(__file__)
        sys.exit()

