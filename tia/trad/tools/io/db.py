import logging
import sys
import tables
import collections
import os

from tia.trad.tools.io.file_handlers import decoratorLockFile, createDir
import tia.trad.market.m as Mm
import tia.configuration as Mfn
from tia.trad.tools.ipc.naming_conventions import IPC


LOGGER_NAME = "rl." + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)  # don't change!


class Depth(tables.IsDescription):
    # market already by structuring the db
    # type already by structuring the db
    date = tables.Int64Col()
    price = tables.Int64Col()
    amount = tables.Int64Col()
class Trades(tables.IsDescription):
    # market already by structuring the db
    date = tables.Int64Col()
    price = tables.Int64Col()
    amount = tables.Int64Col()
    tid = tables.Int64Col()
    type = tables.Int16Col()
    properties = tables.Int16Col()
class Events(tables.IsDescription):
    marketName = tables.StringCol(15)
    date = tables.Int64Col()
    price = tables.Int64Col()
    amount = tables.Int64Col()
    type = tables.Int16Col()
    tid = tables.Int64Col()
    properties = tables.Int16Col()

def fillOrderbook(_orderbookD_, _bidaskS, _priceI, _amountI, _marketS, _date=None, _bidask=None):
    """
    updates or inserts price:amount in appropriate container,
     if amount = 0, deletes price for that amount
    :param _orderbookD_: {IPC.asks: {}, IPC.bids: {}}
    :param _bidaskS: IPC.asks/IPC.bids
    :param _price: int
    :param _amount: int
    :return: _orderbookD_
    """
    try:
        assert _orderbookD_.has_key(IPC.asks), _orderbookD_
        assert _bidaskS in [IPC.asks, IPC.bids], _bidaskS
        validate = type(_priceI); assert validate == int, validate
        validate = type(_amountI); assert validate == int, validate
        try:
            _orderbookD_[_bidaskS][_priceI] += _amountI
        except KeyError:_orderbookD_[_bidaskS][_priceI] = _amountI
        except Exception: raise

        if _orderbookD_[_bidaskS][_priceI] == 0: del _orderbookD_[_bidaskS][_priceI]
        elif _orderbookD_[_bidaskS][_priceI] < 0: logger.critical("Negative amount in orderbook: %s, %s, %s" % (_orderbookD_, _priceI, _marketS))
    except Exception: raise

@decoratorLockFile(Mfn.DB_FILENAME)
def getMarketsToCheck(_h5file=None):
    validate = _h5file; assert validate != None, validate

    marketsToCheck_ = {}
    for marketS in _h5file.root._v_groups:
        marketsToCheck_[marketS] = None
    return marketsToCheck_
def decoratorCheckDeltasIntegrity(func):
    """
    #@attention: it's assumed that cpA/B will not be empty on ANY event in Q for marketX
    intended to decorate DB.writeData
    """
    def funcWrapper(self):
        try:
            logger.debug("checkDeltasIntegrity: hi")
            # get market names to work on
            marketsToCheck = self.marketsSD

            # get last rows and assign lastDates
            lastRowsD = self.getRow(marketsToCheck, "last", "cpA")
            for marketS in marketsToCheck:
                if lastRowsD[marketS] != None:
                    marketsToCheck[marketS] = lastRowsD[marketS]["date"]

            # write new data
            #@attention: it's assumed that cpA/B will not be empty on ANY event in Q for marketX

            func(self)

            # get dates from which to compare deltas with checkpoints
            firstRowsD = {}
            # if any of markets with empty checkpoint tables on prev call, call again for first rows
            if any(["gotNoneVal" for lastDate in marketsToCheck.values() if lastDate == None]):
                firstRowsD = self.getRow(marketsToCheck, "first", "cpA")   # check all that has been written from the queue

                for marketS in marketsToCheck:  # for those with None val, insert lastDate from firstRowsD, others should have lastDate from lastRowsD
                    if marketsToCheck[marketS] == None:
                        if firstRowsD[marketS] != None:
                            marketsToCheck[marketS] = firstRowsD[marketS]["date"]
            # compare orderbooks
            for marketS in marketsToCheck:
                fromDate = marketsToCheck[marketS]
                while 1:
                    try:
                        if fromDate:
                            d1 = self.fillOrderbookFromDeltas(fromDate, marketS); o1 = d1["orderbook"]; nextCheckpointDate = d1["nextDate"]
                            if nextCheckpointDate:
                                self.logger.debug("checkDeltasIntegrity: checking diff")
                                d2 = self.loadOrderbookFromCheckpoint(nextCheckpointDate, marketS); o2 = d2["orderbook"]
                                diffD = dicComparison(o1, o2)
                                # CHECK: should be no difference between loaded from cp & loaded from deltas
                                report = [1 for bidsasks in [IPC.asks, IPC.bids] if any([len(x) for x in diffD[bidsasks]])]
                                if report: print("diffD:%s: %s" % (marketS, diffD))
                                fromDate = nextCheckpointDate
                            else: break
                        else: break
                    except Exception: raise
            self.logger.debug("checkDeltasIntegrity: bye")
        except Exception: raise
    return funcWrapper
class DB:
    logger = logging.getLogger(LOGGER_NAME + ".DB")

    def __init__(self):
        try:
            self.fileName = Mfn.DB_FILENAME
            self.filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
            self.buffer = collections.deque()
            # get all markets we're working on
            self.marketsSD = Mm.getSubclasses(Mm)
            # change default values in dict to None
            for marketName in self.marketsSD: self.marketsSD[marketName] = None
            # create DB
            if not os.path.exists(Mfn.DB_FILENAME):
                createDir("/db")
                for marketName in self.marketsSD: self.createTables(marketName)
        except Exception: raise

    @decoratorLockFile(Mfn.DB_FILENAME)
    def updateEventsTable(self):
        # or just create new one
        pass
    @decoratorLockFile(Mfn.DB_FILENAME)
    def createEventsTable(self, _h5file=None):
        try:
            self.logger.debug("createEventsTable: hi")

            """ create new events.h5 """
            h5tmp = tables.openFile("/run/shm/events.h5", mode="a", filters=self.filters)
            h5events = tables.openFile("db/events.h5", mode="a", filters=self.filters)
            # create checkpoints tables
            for marketS in self.marketsSD:
                if marketS not in Mfn.DEAD_MARKETS:
                    h5events.createGroup("/", marketS, marketS, filters=self.filters)
                    group = getattr(h5events.root, marketS)
                    h5events.createTable(group, "cpA", Depth, "checkpoints", filters=self.filters)
                    h5events.createTable(group, "cpB", Depth, "checkpoints", filters=self.filters)
            # create events table
            h5tmp.createTable(h5tmp.root, "events", Events, "CSIsorted_events", filters=self.filters)
            eventsT = h5tmp.root.events; eventsR = eventsT.row
            # copy data
            for marketS in self.marketsSD:
                if marketS not in Mfn.DEAD_MARKETS:
                    sys.stderr.write("copying deltas for %s\n" % marketS)

                    group = getattr(_h5file.root, marketS)
                    asksT = getattr(group, "asks")
                    bidsT = getattr(group, "bids")
                    tradesT = getattr(group, "trades")
                    decisionD = {asksT: IPC.normAsk, bidsT: IPC.normBid, tradesT: None}
                    # copy all deltas to /events
                    for keyTable in decisionD:
                        for index, row in enumerate(keyTable):
                            try:
                                eventsR["marketName"] = marketS
                                eventsR["date"] = row["date"]
                                eventsR["price"] = row["price"]
                                eventsR["amount"] = row["amount"]
                                # also copy trades
                                if keyTable == tradesT:
                                    type = row["type"]
                                    tid = row["tid"]
                                    props = row["properties"]
                                else:
                                    type = decisionD[keyTable]
                                    tid = 0
                                    props = 0
                                eventsR["type"] = type
                                eventsR["tid"] = tid
                                eventsR["properties"] = props
                                eventsR.append()
                                if index % 10000 == 0: eventsT.flush()
                            except Exception: raise
                        eventsT.flush()
            # create index
            sys.stderr.write("\ncreating indices")
            col = tables.Column(eventsT, "date", eventsT.description)
            col.createCSIndex(filters=self.filters, tmp_dir="/run/shm")
            h5events.close(); h5tmp.close()
            # copy data from h5tmp and sort id by "date" column
            sys.stderr.write("\nwriting sorted events")
            os.system('ptrepack --sortby="date" --chunkshape="auto" /run/shm/events.h5:/events db/events.h5:/events')
            os.system('rm /run/shm/events.h5')
            # index sorted events table
            h5events = tables.openFile("db/events.h5", mode="a", filters=self.filters)
            eventsT = h5events.root.events
            col = tables.Column(eventsT, "date", eventsT.description)
            col.createCSIndex(filters=self.filters, tmp_dir="/run/shm")
            h5events.close()

            self.logger.debug("createEventsTable: bye")
        except Exception: raise

    def validateData(self):
        try:
            firstRowsD = self.getRow(self.marketsSD, "first", "cpA")
            lastRowsD = self.getRow(self.marketsSD, "last", "cpA")
            for marketS in firstRowsD:
                if firstRowsD[marketS]:
                    fromDate = firstRowsD[marketS]["date"]
                    lastDate = lastRowsD[marketS]["date"]
                    # check each etape
                    while 1:
                        try:
                            d1 = self.fillOrderbookFromDeltas(fromDate, marketS); o1 = d1["orderbook"]; nextCheckpointDate = d1["nextDate"]
                            if nextCheckpointDate:
                                d2 = self.loadOrderbookFromCheckpoint(nextCheckpointDate, marketS); o2 = d2["orderbook"]

                                diffD = dicComparison(o1, o2)
                                # CHECK: should be no difference between loaded from cp & loaded from deltas
                                for bidsasks in ["asks", "bids"]:
                                    if any([len(x) for x in diffD[bidsasks]]):
                                        print("diff: %s:from: %s:diff = %s" % (marketS, fromDate, diffD))
                                        break

                                fromDate = nextCheckpointDate
                            else: break
                        except Exception: raise
                    # check from first firstCp->fillFrDeltas->lastCp
                    firstWithAllDeltasOrd = self.fillOrderbookFromDeltas(fromDate, marketS, _lastCpDate=lastDate)["orderbook"]
                    lastOrd = self.loadOrderbookFromCheckpoint(lastDate, marketS)["orderbook"]
                    diffD = dicComparison(firstWithAllDeltasOrd, lastOrd)
                    for bidsasks in ["asks", "bids"]:
                        if any([len(x) for x in diffD[bidsasks]]):
                            print("wholeDeltas: %s:from: %s:diff = %s" % (marketS, fromDate, diffD))
                            break
                    sys.stderr.write("%s validated: ok\n" % marketS)
                else: sys.stderr.write("\n\t%s missing" % marketS)
            print
        except Exception: raise
    @decoratorLockFile(Mfn.DB_FILENAME)
    def getRow(self, _marketsD, _firstLastS, _nodeS, _h5file=None):
        """
        returns first or last rows of all groups for node _nodeS
        :param: _marketsL: [str(marketName)]
        :param: _firstLastS: str; must be one of valid_firstLastS
        :param _nodeS: str; must be one of valid_nodeS
        :param _h5file: h5object; db file
        :return: {marketS: lastRow}
        """
        try:
            self.logger.debug("getRow: hi")

            # check if input valid
            valid_firstLastS = ["first", "last"]
            if _firstLastS not in valid_firstLastS: raise Exception("getRow: wrong input! Expected one of %s, but got: %s", [valid_firstLastS, _firstLastS])
            valid_nodeS = ["cpA", "cpB", IPC.asks, IPC.bids, "trades"]
            if _nodeS not in valid_nodeS: raise Exception("getRow: wrong input! Expected one of %s, but got: %s",  [valid_nodeS, _nodeS])
            validate = _h5file; assert validate != None, validate

            rowsD_ = {}
            groups = _h5file.root._v_groups     # returns {'str(groupName)': getattr(_h5file.root, groupName)}

            for marketS in _marketsD:
                group = groups[marketS]
                table = getattr(group, _nodeS)

                if table.nrows:
                    if _firstLastS == "first": rowsD_[marketS] = [row for row in table.iterrows(0, 1)][0]
                    elif _firstLastS == "last": rowsD_[marketS] = [row for row in table.iterrows(table.nrows - 1, table.nrows)][0]
                    # check that we get what we want
                    returnedType = type(rowsD_[marketS])
                    if returnedType != tables.tableExtension.Row: raise AssertionError(
                        "getRow: expected type='tables.tableExtension.Row', but got type: %s", returnedType)
                else: rowsD_[marketS] = None

            self.logger.debug("getRow: bye")
            return rowsD_
        except Exception: raise

    @decoratorLockFile(Mfn.DB_FILENAME)
    def loadOrderbookFromCheckpoint(self, _fromDate, _marketS, _h5file=None):
        """
        return orderbook filled from h5file.root.market.cpA&cpB and date of next checkpoint
        :param _fromDate: int; from which date on load checkpoints
        :param _marketS: str; group name
        :param _h5file: h5object; db file
        :return: {"orderbook":{IPC.asks: {data}, IPC.bids: {data}|None, "nextDate":data|None}
        """
        try:
            self.logger.debug("loadOrderbookFromCheckpoint: hi")

            validate = _h5file; assert validate != None, validate

            orderbook_ = {IPC.asks: {}, IPC.bids: {}}; nextDate_ = None
            group = getattr(_h5file.root, _marketS)
            decisionD = {IPC.asks: getattr(group, "cpA"), IPC.bids: getattr(group, "cpB")}

            # fill orderbook_
            highestRowNumber = 0; where = None
            for bidsasks in decisionD:
                table = decisionD[bidsasks]
                for row in table.where('date == _fromDate'):
                    if row["amount"] > 0:
                        orderbook_[bidsasks][row["price"]] = row["amount"]

                    if row.nrow > highestRowNumber:
                        highestRowNumber = row.nrow; where = table

            # scenario:nodata|anomalousdata
            if all([not orderbook_[IPC.asks], not orderbook_[IPC.bids]]):   # if both don't have data
                orderbook_ = None
            elif not(orderbook_[IPC.asks] and orderbook_[IPC.bids]):    # if one has data but other doesn't
                raise Exception("loadOrderbookFromCheckpoint: asks or bids have no data. Got: ", orderbook_)

            # get next date
            nxDate = [row["date"] for row in where.iterrows(highestRowNumber + 1, highestRowNumber + 2)]     # returns empty list if at the end
            # scenario:nodata
            nextDate_ = nxDate[0] if nxDate else None

            self.logger.debug("loadOrderbookFromCheckpoint: bye")
            return {"orderbook": orderbook_, "nextDate": nextDate_}
        except Exception: raise

    @decoratorLockFile(Mfn.DB_FILENAME)
    def suppFillOrderbookFromDeltas(self, _startDate, _marketS, d, _lastCpDate, _h5file=None):
        try:
            validate = _h5file; assert validate != None, validate

            orderbook_ = d["orderbook"]; nextCheckpointDate_ = d["nextDate"]
            if (orderbook_ != None): # and (nextCheckpointDate_ != None):
                group = getattr(_h5file.root, _marketS)
                # choose deltas table
                decisionD = {IPC.asks: getattr(group, IPC.asks), IPC.bids: getattr(group, IPC.bids)}
                # choose condition for selecting rows in table
                tablesConditionS = None
                if _lastCpDate: tablesConditionS = '(_startDate < date) & (date <= _lastCpDate)'
                # the else is needed for continuing from old DB so that we can start from last cp and include deltas
                # that come after that cp
                else: tablesConditionS = '(_startDate < date) & (date <= nextCheckpointDate_)' if nextCheckpointDate_ else '(_startDate < date)'

                if tablesConditionS == None: raise Exception("TablesConditions == None")
                for bidsasks in decisionD:
                    table = decisionD[bidsasks]
                    for row in table.where(tablesConditionS):
                        fillOrderbook(orderbook_, bidsasks, row["price"], row["amount"], _marketS, row["date"], bidsasks)

            return {"orderbook": orderbook_, "nextDate": nextCheckpointDate_}
        except Exception: raise
    def fillOrderbookFromDeltas(self, _startDate, _marketS, _lastCpDate=None):
        """
        returns orderbook from checkpoints updated with deltas till next checkpoint
        :param _startDate: int; loading orderbook from that checkpoint
        :param _marketS: str; group
        :param _h5file: h5object; db file
        :return: {"orderbook": {IPC.asks: {data}, IPC.bids: {data}}|None, "nextDate": _nextCheckpointDate|None}
        """
        try:
            self.logger.debug("fillOrderbookFromDeltas: hi")

            d = self.loadOrderbookFromCheckpoint(_startDate, _marketS)
            resultD = self.suppFillOrderbookFromDeltas(_startDate, _marketS, d, _lastCpDate)

            self.logger.debug("fillOrderbookFromDeltas: bye")
            return {"orderbook": resultD["orderbook"], "nextDate": resultD["nextDate"]}
        except Exception: raise

    @decoratorLockFile(Mfn.DB_FILENAME)
    def createTables(self, _groupName, _h5file=None):
        try:
            self.logger.debug("createTables: hi. creating %s", _groupName)

            try:
                validate = _h5file; assert validate != None, validate

                getattr(_h5file.root, _groupName)  # throws AttributeError if attribute doesn't exist
            except AttributeError:
                _h5file.createGroup("/", _groupName, _groupName)

            try:
                newGroup = getattr(_h5file.root, _groupName)
                getattr(newGroup, IPC.asks)  # throws AttributeError if attribute doesn't exist
            except AttributeError:
                _h5file.createTable(newGroup, IPC.asks, Depth, "asks_deltas", filters=self.filters, expectedrows=1000000000)
            try:
                getattr(newGroup, "cpA")  # throws AttributeError if attribute doesn't exist
            except AttributeError:
                _h5file.createTable(newGroup, "cpA", Depth, "checkpoints", filters=self.filters, expectedrows=1000000000)

            try:
                getattr(newGroup, IPC.bids)  # throws AttributeError if attribute doesn't exist
            except AttributeError:
                _h5file.createTable(newGroup, IPC.bids, Depth, "bids_deltas", filters=self.filters, expectedrows=1000000000)
            try:
                getattr(newGroup, "cpB")  # throws AttributeError if attribute doesn't exist
            except AttributeError:
                _h5file.createTable(newGroup, "cpB", Depth, "checkpoints", filters=self.filters, expectedrows=1000000000)

            try:
                getattr(newGroup, "trades")
            except AttributeError:
                _h5file.createTable(newGroup, "trades", Trades, "trades", filters=self.filters, expectedrows=10000000)

            self.logger.debug("createTables: bye")
        except Exception: raise

    def writeTrades(self, _item, _h5file):
        try:
            validate = _item.__class__.__name__; assert validate == IPC.IPCTrade, validate
            # select table
            group = getattr(_h5file.root, _item.market); table = getattr(group, "trades")
            # write data
            table.row["date"] = _item.date
            table.row["price"] = _item.price
            table.row["amount"] = _item.amount
            table.row["tid"] = _item.tid
            table.row["type"] = _item.type
            table.row["properties"] = _item.properties
            table.row.append()
            table.flush()
        except Exception: raise
    def writeCheckpoints(self, _item, _h5file):
        try:
            validate = _item.__class__.__name__; assert validate == IPC.IPCOrderbook, validate
            # select table
            group = getattr(_h5file.root, _item.market)
            data = [[_item.orderbook[IPC.asks], getattr(group, "cpA")], [_item.orderbook[IPC.bids], getattr(group, "cpB")]]
            # write data
            for tpl in data:
                bidsasksD = tpl[0]; table = tpl[1]
                for price in bidsasksD:
                    try:
                        table.row["date"] = _item.date
                        table.row["price"] = price
                        table.row["amount"] = bidsasksD[price]
                        table.row.append()
                    except Exception: raise
                table.flush()
        except Exception: raise
    def writeDeltas(self, _item, _h5file):
        try:
            validate = _item.__class__.__name__; assert validate == IPC.IPCDelta, validate
            # select table
            group = getattr(_h5file.root, _item.market)
            bidsAsksS = _item.typeNorm2Str(_item.type)
            table = getattr(group, bidsAsksS)
            # append data
            table.row["date"] = _item.date
            table.row["price"] = _item.price
            table.row["amount"] = _item.delta
            table.row.append()
            # flush to disk
            table.flush()
        except Exception: raise
    @decoratorCheckDeltasIntegrity
    @decoratorLockFile(Mfn.DB_FILENAME)
    def flushBuffer(self, _h5file=None):
        """
        writes data to DB file from buffer self.buffer
        inout: {"mode": ("checkpoint"|"delta"|"trade"),"date": int, "market": str,
            (IPC.bids: {}, IPC.asks: {}) or
            ("delA": {}, "delB": {}) or
            ("trades": [])}
        """
        try:
            self.logger.debug("flushBuffer: hi")

            validate = _h5file; assert validate != None, validate
            # flush buffer
            while 1:
                try:
                    item = self.buffer.popleft()  # FIFO queue
                    mode = item.__class__.__name__
                    if mode == IPC.IPCDelta: self.writeDeltas(item, _h5file)
                    elif mode == IPC.IPCOrderbook: self.writeCheckpoints(item, _h5file)
                    elif mode == IPC.IPCTrade: self.writeTrades(item, _h5file)
                    else: raise Exception("writeData: unknown mode: %s", mode)
                except IndexError:
                    break
                except Exception: raise

            self.logger.debug("flushBuffer: bye")
        except Exception: raise



