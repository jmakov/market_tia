import sys
import time
from tia.trad.tools.io.follow import followMonitor
import tia.configuration as conf
from tia.trad.tools.errf import eReport
import ujson as json
import matplotlib.pyplot as plt
import math
import collections
import logging
from tia.trad.tools.ipc.processLogger import PROCESS_NAME

LOGGER_NAME = PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
reportFile = None


def pointDistance(initF, initI, point):
    try:
        t = initI[0]-initF[0], initI[1]-initF[1]           # Vector ab
        dd = math.sqrt(t[0]**2+t[1]**2)         # Length of ab
        t = t[0]/dd, t[1]/dd               # unit vector of ab
        n = -t[1], t[0]                    # normal unit vector to ab
        ac = point[0]-initF[0], point[1]-initF[1]          # vector ac
        return math.fabs(ac[0]*n[0]+ac[1]*n[1]) # Projection of ac to n (the minimum distance)
    except Exception: raise

def getAvg(_list):
    try:
        return float(max(_list) + min(_list)) / float(2)
    except Exception: raise


def shutdown():
    try:
        logger.debug("shutting down")
        global reportFile
        reportFile.close()
    except Exception: raise

def run(**kwargs):
    try:
        global logger
        global reportFile
        logger = kwargs["processLogger"]
        logger.debug("monitor_mainTr:hi")
        _initFunds = kwargs["initFunds"]
        _initItems = kwargs["initItems"]

        plt.ion()   # turn interactive on
        fig = plt.figure()
        fig.show()
        # raw
        ax = fig.add_subplot(221)
        #hline = ax.axhline(y=_initFunds)
        #vline = ax.axvline(x=_initItems)
        #ax.set_xscale("log")
        #ax.set_yscale("log")
        data, = ax.plot([], [], 'b+')
        data11, = ax.plot([], [], 'ro')
        # value
        ax2 = fig.add_subplot(222)
        data2, = ax2.plot([], [], 'ro-')

        # inside TM
        ax3 = fig.add_subplot(223)
        data3, = ax3.plot([], [], 'ro')
        data4, = ax3.plot([],[], 'bo')

        minBids, = ax3.plot([], [], "r>")
        maxAsks, = ax3.plot([], [], "b>")
        # top b/a
        ax5 = fig.add_subplot(224)
        dataI, = ax5.plot([], [], "o-")
        dataF, = ax5.plot([], [], "ro-")

        windowLength = 50
        fundsHistory = collections.deque(maxlen=windowLength); itemsHistory = collections.deque(maxlen=windowLength)
        valueHistory = collections.deque(maxlen=windowLength)
        tmFundsHistory = collections.deque(maxlen=windowLength); tmItemsHistory = collections.deque(maxlen=windowLength)


        tmIAHSum = collections.deque(maxlen=windowLength); tmFAHSum = collections.deque(maxlen=windowLength)

        topAsksHistory = collections.deque(maxlen=10)
        topBidsHistory = collections.deque(maxlen=10)
        # touch report.json
        #reportFile = open(conf.FN_REPORT, "w"); reportFile.close()
        reportFile = open(conf.FN_REPORT, "r")
        newline = followMonitor(reportFile, fig)


        while 1:
            try:
                #for line in reportFile:
                line = newline.next()
                jsonObj = json.loads(line)

                universeSize = float(jsonObj["universeSize"])
                topAsks = jsonObj["topAsks"]; topBids = jsonObj["topBids"]

                initInvF = float(_initFunds) * universeSize
                initInvI = float(_initItems) * universeSize
                cumulFunds = float(jsonObj["cumulFunds"])
                cumulItems = float(jsonObj["cumulItems"])
                #fundsHistory.append(funds); itemsHistory.append(items)

                dist = pointDistance([0, initInvF], [initInvI, 0], [cumulFunds, cumulItems])
                fundsHistory.append(dist)

                #data.set_ydata(fundsHistory); data.set_xdata(itemsHistory)
                data.set_ydata(fundsHistory); data.set_xdata(xrange(len(fundsHistory)))
                #data11.set_ydata(funds); data11.set_xdata(items)
                #data11.set_ydata(dist); data11.set_xdata(xrange(len(fundsHistory)))
                ax.relim()
                ax.autoscale_view(True,True,True)

                tmFunds = jsonObj["tmFunds"]; tmItems = jsonObj["tmItems"]
                tmFA = 0; tmIA = 0
                tmFPH = collections.deque(); tmFAH = collections.deque()
                tmIPH = collections.deque(); tmIAH = collections.deque()
                for price in tmFunds:
                    amount = tmFunds[price]
                    tmFPH.append(price)
                    tmFAH.append(amount)
                    tmFA += amount
                tmFAHSum.append(tmFA)

                for price in tmItems:
                    amount = tmItems[price]
                    tmIPH.append(price)
                    tmIAH.append(amount)
                    tmIA += amount
                tmIAHSum.append(tmIA)

                dataI.set_ydata(tmIAHSum); dataI.set_xdata(xrange(len(tmIAHSum)))
                dataF.set_ydata(tmFAHSum); dataF.set_xdata(xrange(len(tmFAHSum)))
                ax5.relim()
                ax5.autoscale_view(True,True,True)

                value = float(jsonObj["value"]) / initInvF if initInvF else float(jsonObj["value"])
                valueHistory.append(value)
                data2.set_xdata(range(len(valueHistory)))
                data2.set_ydata(valueHistory)
                ax2.relim()
                ax2.autoscale_view(True,True,True)


                """
                TM stuff
                """
                # make universe states pretty
                tmpList = list(tmFAH) + list(tmIAH)
                xDrawStart = min(tmpList)
                drawedInterval = max(tmpList) - xDrawStart
                spacing = float(drawedInterval) / float (len(topBids))
                offset = float(spacing) / float(2)
                xcords = collections.deque()
                for index, bid in enumerate(topBids):
                    xcords.append(offset + xDrawStart + index * spacing)
                minBids.set_ydata(topBids); minBids.set_xdata(xcords)
                maxAsks.set_ydata(topAsks); maxAsks.set_xdata(xcords)

                data3.set_xdata(tmFAH)
                data3.set_ydata(tmFPH)
                data4.set_xdata(tmIAH)
                data4.set_ydata(tmIPH)
                ax3.relim()
                ax3.autoscale_view(True,True,True)

                fig.canvas.draw()
                #plt.savefig(conf.FN_PLOT_IMAGE)
            except ValueError: continue
    except Exception as ex:
        eReport(__file__)
        reportFile.close()
        sys.exit()
