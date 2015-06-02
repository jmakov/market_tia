import lib as fl
import lib.old.tr_market_variables
import sys
import tables
import json
import time     #for progress report in getMtgoxTrades
import os
#import ujson as json     #don't use for fromFileList since it only parses from strings
from lib.old.exchanges import getHtml

def stage3():
    #delA, delB, trades v events, index, eventsSorted
    """
    write deltas for masks AND mbids.h5
    copy trades, delA, delB to events and build eventsSorted.h5
    """
    try:
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
        
        #masks/mbids.h5 not indexed. build index and write deltas for mAsks, mBids.h5:
        sys.stderr.write("\nstart: building indexes")            
        mAF = tables.openFile("_misc/db/masks.h5", mode="a"); mAT = mAF.root.mtgoxUSD.asks 
        mBF = tables.openFile("_misc/db/mbids.h5", mode="a"); mBT = mBF.root.mtgoxUSD.bids
        
        nameD = {mAT: "asks", mBT: "bids"}
        for mT in [mAT, mBT]:
            #build index
            sys.stderr.write("\nstart: writing index")
            col = tables.Column(mT, "eventDate", mT.description)  #build index by eventDate column            
            if col.is_indexed:
                col.removeIndex()
            col.createCSIndex(db_filters, tmp_dir="/run/shm")   #build tmp file index in RAM
            
            #write deltas
            sys.stderr.write("\nstart: writing deltas")     
            writeDeltas("_misc/db/del" + nameD[mT] + ".h5", mT, nameD[mT])
        
        #now we got deltas
        delAF = tables.openFile("_misc/db/delasks.h5", mode="r"); delAT = delAF.root.events
        delBF = tables.openFile("_misc/db/delbids.h5", mode="r"); delBT = delBF.root.events
        
        #create events DB
        eventsF =  getMasterDB("_misc/db/events.h5", db_filters, "mtgoxUSD"); eventsT = eventsF.root.events
                     
        #copy trades
        sys.stderr.write("\nstart: copy trades")
        tradesF = tables.openFile("_misc/db/trades.h5", mode="r"); tradesT = tradesF.root.mtgoxUSD.trades
        nrows = tradesT.nrows
        for index, row in enumerate(tradesT):
            eventsT.row["eventDate"] = row["eventDate"]
            eventsT.row["market"] = "mtgoxUSD"
            eventsT.row["price"] = row["price"]
            eventsT.row["amount"] = row["amount"]
            eventsT.row["tradeID_orderType"] = row["tradeID"]
            eventsT.row.append()
            if index%10000==0:
                sys.stderr.write("\ncopying trades: %.8f" % (float(index) / float(nrows)))
                eventsT.flush()
        eventsT.flush()
        tradesF.close()
        
        #copy delA
        sys.stderr.write("\nstart: copy delA"); nrows = delAT.nrows
        for index, row in enumerate(delAT):
            eventsT.row["eventDate"] = row["eventDate"]
            eventsT.row["market"] = row["market"]
            eventsT.row["price"] = row["price"]
            eventsT.row["amount"] = row["amount"]
            eventsT.row["tradeID_orderType"] = row["tradeID_orderType"]
            eventsT.row.append()
            if index%10000==0:
                sys.stderr.write("\ncopying delA: %.8f" % (float(index) / float(nrows)))
                eventsT.flush()
        eventsT.flush()
        delAF.close()
        
        #copy delB
        sys.stderr.write("\nstart: copy delB"); nrows = delBT.nrows
        for index, row in enumerate(delBT):
            eventsT.row["eventDate"] = row["eventDate"]
            eventsT.row["market"] = row["market"]
            eventsT.row["price"] = row["price"]
            eventsT.row["amount"] = row["amount"]
            eventsT.row["tradeID_orderType"] = row["tradeID_orderType"]
            eventsT.row.append()
            if index%10000==0:
                sys.stderr.write("\ncopying delB: %.8f" % (float(index) / float(nrows)))
                eventsT.flush()
        eventsT.flush()
        delBF.close()
        
        #build index
        sys.stderr.write("\nstart: building indexes")            
        col = tables.Column(eventsT, "eventDate", eventsT.description)  #build index by eventDate column
        if col.is_indexed:
            col.removeIndex()
        col.createCSIndex(db_filters, tmp_dir="/run/shm")   #build tmp file index in RAM
        eventsF.close()
        
        #build sorted table
        sys.stderr.write("\nstart: building eventsSorted")
        os.system('ptrepack  --sortby="eventDate" --chunkshape="auto" --keep-source-filters _misc/db/events.h5:/events _misc/db/eventsSorted.h5:/events')

        
        
    except Exception as ex:
        tradesF.close(); delAF.close(); delBF.close(); eventsF.close()
        sys.stderr.write("\stage3:ex: %s" % ex)
def stage2():
    """
    creates masks.h5 or mbids.h5 which contain all the asks/bids from available data
    """
    try:
        table = raw_input("asks/bids?: ")
        
        fileList = ["_misc/db/allFilesList.h5"]
        
        groupName = "mtgoxUSD"
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)        
        mh5file =  getMasterDB("_misc/db/m" + table + ".h5", db_filters, groupName); mT = getattr(mh5file.root.mtgoxUSD, table)
        
        for myFile in fileList:
            h5file = tables.openFile(myFile, mode="r"); aT = getattr(h5file.root.mtgoxUSD, table)
            nrows = aT.nrows
            for index, row in enumerate(aT):
                mT.row["eventDate"] = row["eventDate"]
                mT.row["price"] = row["price"]
                mT.row["amount"] = row["amount"]
                mT.row.append()
                
                if index%100000 == 0:
                    sys.stderr.write("\t%.6f" % (float(index) / float(nrows)))
                    mT.flush()
            
                #if index==1267009:
                #    sys.stderr.write("\n index: %i, eDAte: %i, pr: %i, am: %i" % (index, row["eventDate"], row["price"], row["amount"]))
            mT.flush()
            h5file.close()
       
      
        mh5file.close()
    except Exception as ex:
        sys.stderr.write("\ngoueMtgoxUSDTables:ex: %s" % ex)
"""
stage1:
run writeSecond a bunch of times on split -l 90000 allFiles.txt io = xaa, xab etc.
"""

def writeTradesH5(_h5file, _tradesL):
    try:
        tradesT = _h5file.root.mtgoxUSD.trades
        for dic in _tradesL:
            try:
                tradesT.row["eventDate"] = dic["tid"]                
                tradesT.row["tradeID"] = dic["tid"]
                tradesT.row["price"] = dic["price_int"] 
                tradesT.row["amount"] = dic["amount_int"]
                tradesT.row.append()
            except Exception as ex:
                sys.stderr.write("\nwriteTradesH5:in writing: %s" % ex)
        tradesT.flush()                 
    except Exception as ex:
        sys.stderr.write("\nwriteTradesH5:ex: %s" % ex)
        _h5file.close()
def loadTrades(_jsonObj):
    """
    amount     the traded amount in item (BTC), float, deprecated
    amount_int     the traded amount * 1E8
    date     unix timestamp of trade
    item     What was this trade about
    price     price per unit, float, deprecated
    price_int     price in smallest unit as integer (5 decimals of USD, 3 in case of JPY)
    price_currency     currency in which trade was completed
    tid     Trade id (big integer, which is in fact trade timestamp in microseconds)
    trade_type     Did this trade result from the execution of a bid or a ask?
    """
    try:
        verifyD = {"price_currency": "USD", "primary": "Y"}
        tradesL_ = []
        if _jsonObj["result"] == "success":
            for dic in _jsonObj["return"]:
                try:
                    if all([True for key in verifyD.keys() if dic[key]==verifyD[key]]):     #verify that the data we're attempting to write
                        tradesL_.append(dic)                                     #comes from a json with fields and cals as in verifyD
                except KeyError as ex:
                    sys.stderr.write("\nloadTrades:in testing4membership: %s" % ex)
                    pass
        else:
            sys.stderr.write("\nloadTradesr: result!=success: %s" % _jsonObj["result"])
        
        return tradesL_     #returns a list of dictionaries
    except Exception as ex:
        sys.stderr.write("loadTrades:ex: %s" % ex)
def getMtgoxTrades():
    try:
        h5FileName = raw_input("DB name: ")
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
        h5file = getMasterDB(h5FileName, db_filters, "mtgoxUSD")
        
        getTradesSince = 1311520521     #3h before first eventDate in mtgoxUSD.asks
        nowTime = time.time()
        
        _sinceTimeStamp = getTradesSince * 10**6   #mtgox uses microsecond timestamp (int) as tradeid 
        while 1:
            try:
                html = getHtml("https://mtgox.com/api/1/BTCUSD/trades?since=" + str(_sinceTimeStamp))
                    
                jsonObj= json.loads(html)
                if jsonObj:
                    tradesL = loadTrades(jsonObj)
                    writeTradesH5(h5file, tradesL)
                else:
                    raise ValueError
                
                if len(tradesL) == 0:   #if no more trades since _sinceTimeStamp, stop
                    sys.stderr.write("\ngetMtgoxTrader: tradesL empty. If timestamp matches date -%s, we got all trades till now.")
                    break
                
                _sinceTimeStamp = tradesL[len(tradesL) - 1]["tid"]  #get latest tid from json so we can get trades since that tid
                
                """progress"""
                sys.stderr.write("\t %.7f" % (1 - float(_sinceTimeStamp) / float(nowTime*10**6)))
            except Exception as ex:
                sys.stderr.write("\ngetMtgoxTrades: while:ex: %s" % ex)
        h5file.close()
    except Exception as ex:
        sys.stderr.write("\ngetMtgoxtrades:ex: %s" % ex)
        h5file.close()

def chkDeltasConsistency(_deltaT):
    """
    shuld be already sorted since we append in ascending order
    """
    try:
        nrows = _deltaT.nrows
        orderbook = {}
        #no need to iterating through the table in sorted order since for our table, the deltas are written sorted
        for index, row in enumerate(_deltaT):
            try:
                orderbook[row["price"]] += row["amount"]
                if orderbook[row["price"]] == 0:
                    del orderbook[row["price"]]
                elif orderbook[row["price"]] < 0:
                    sys.stderr.write("\n index: %i, eDAte: %.10f, pr: %.8f, am: %.12f" % (index, row["eventDate"], row["price"], row["amount"]))
                    sys.exit()
            except KeyError:
                orderbook[row["price"]] = row["amount"]
                if orderbook[row["price"]] < 0:
                    sys.stderr.write("\n index: %i, eDAte: %.10f, pr: %.8f, am: %.12f" % (index, row["eventDate"], row["price"], row["amount"]))
                    sys.exit()
                
            if index%100000==0:
                sys.stderr.write("\nintegrity:%.6f" % (float(index) / float(nrows)))
                
    except Exception as ex:
        sys.stderr.write("\nchkDeltasConsistency:ex: %s" % ex)
def writeToDB(_groupName, _deltas, _eventsT, _dateCurr, _orderType):
    """
    write deltas to table
    implementation: we already filtered deltaAmount = 0 out so no need for another if loop
    """
    try:
        for key in _deltas:  
            try:
                _eventsT.row["market"] = _groupName
                _eventsT.row["eventDate"] = _dateCurr
                _eventsT.row["price"] = key
                _eventsT.row["amount"] = _deltas[key]
                _eventsT.row["tradeID_orderType"] = _orderType   #write 0 for asks, 1 for bids - look@lib.tr_market_variables.Events
                _eventsT.row.append()
            except Exception as ex:
                sys.stderr.write("\nwriteDeltas:(write to tables): %s" %ex)
                sys.exit()
        #eventsT.flush()  #for performance reasons we flush in writeDeltas:get progress section
    except Exception as ex:
        sys.stderr.write("\nwriteToDB: %s" % ex)
        _eventsT.flush()
def getDelta(_prevD, _currD):
    """
    input: ({pr:am}, {pr,am})
    output: {pr: delta}
    implementation: go through currD:
                        - check if member keys in prevD. if they are, get difference. if diff 0, nothing has changed and we don't need that delta
                        - if key not present in prevD, we interpret that as a whole new order in currD and store that to delta
                    go through prevD:
                        - check what keys left untouched. we interpret that as keys that vanished, since they're not in currD any more.
                        we write to delta negative amount with that key
    """    
    try:
        result_ = {}
        for key in _currD:
            try:
                delta = _currD[key] - _prevD[key]     #delta amount
                if delta != 0:
                    result_[key] = delta
            except KeyError:    
                result_[key] = _currD[key]  #if key not in prevD it's obviously new
        
        vanished = set(_prevD) - set(_currD)    #check which keys are in _prevD but not in _currD. Those apparently vanished.
        for key in vanished:
            result_[key] = -_prevD[key]
        return result_       
    except Exception as ex:
        sys.stderr.write("\ngetDelta: %s" % str(ex))
def writeDeltas(_fileName, _bidaskT, _bidsAsks):
    """
    writes deltas for asks AND bids to h5file.root.events starting with deltas relative to {} - starting point,
    so it's not neccesary to load from initial checkpoint, although checkpoints are planned for
    later intervals
    """
    try:
        groupName = "mtgoxUSD"
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
        h5file = getMasterDB(_fileName, db_filters, groupName)   
        eventsT = h5file.root.events     
        
        orderTypeD = {"asks": 0, "bids": 1}
        dateCurr = [row["eventDate"] for row in _bidaskT.itersorted(sortby="eventDate", checkCSI=True, start=0, stop=1)][0]
        
        nrows = _bidaskT.nrows
        prevD = {}; currD = {}
        for index, row in enumerate(_bidaskT.itersorted(sortby="eventDate", checkCSI=True)):
            dateNext = row["eventDate"]
            if dateCurr != dateNext:
                deltas = getDelta(prevD, currD)
                writeToDB(groupName, deltas, eventsT, dateCurr, orderTypeD[_bidsAsks])
                
                #handle dics
                prevD = currD.copy()    #since we'll need to compare to prev state
                currD = {}  #start filling up bids/asks for new eventDate
                                    
                dateCurr = dateNext     #handle next event
            currD[row["price"]] = row["amount"]   #fill current orderbook
            
            #get progress
            if index % 100000 == 0:
                sys.stderr.write("\nprogress percent: %.8f" % (float(index) / float(nrows)) )             
                eventsT.flush()  #for performance reasons we flush here instead in writeToDb
        eventsT.flush()  #write last 1000000 rows
        chkDeltasConsistency(eventsT)
            
        h5file.close()
    except Exception as ex:
        sys.stderr.write("\nwriteDeltas: %s" % str(ex))
        h5file.close()

def parseJson(_str, _line):
    try:
        asksStr = 'asks":['; bidsStr = '"bids":[' 
        iasksS = _str.find(asksStr); iasksE = _str.find(']]', iasksS + len(asksStr))
        ibidsS = _str.find(bidsStr); ibidsE = _str.find(']]', ibidsS + len(bidsStr))
        
        asksL = []; bidsL = []
        if all([x > 0 for x in [iasksS, ibidsS]]):  #check that json not empty or error msg
            if iasksS < 5:  #check that asks first, bids second
                startI = _str.find("[[") + 2; 
                while startI < iasksE: 
                    endI = _str.find("]", startI)
                    interesting = _str[startI:endI]; comma = interesting.find(",")
                    price = interesting[0:comma]; amount = interesting[comma + 1:]
                    startI = _str.find("[", endI) + 1
                    asksL.append([price, amount])
                
                startI = _str.find("[[", ibidsS) + 2;
                print startI 
                while startI != 0:  #str.find() returns 0 when at the end of the string... 
                    endI = _str.find("]", startI)
                    interesting = _str[startI:endI]; comma = interesting.find(",")
                    price = interesting[0:comma]; amount = interesting[comma + 1:]
                    startI = _str.find("[", endI) + 1
                    bidsL.append([price, amount])    
            else:
                sys.stderr.write("\nasks not first!")
                sys.exit()
        else:
            sys.stderr.write("\n no data: %s" % _line)
        
        return [asksL, bidsL]
    except Exception as ex:
        sys.stderr.write("\nparseJson:ex: %s" % ex)
def writeStatesFromList():
    """
    allFilesList.txt contains paths to json obj which filename is unix timestamp
    """
    try:
        filename = raw_input("io list: ")
        h5FileName = filename + ".h5"        
        
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
        h5file = getMasterDB(h5FileName, db_filters, "mtgoxUSD")
        
        f = open(filename, "r")
        file_size = os.path.getsize(filename)
        
        asksT = h5file.root.mtgoxUSD.asks
        bidsT = h5file.root.mtgoxUSD.bids
        arow = asksT.row
        brow = bidsT.row
         
        for index, line in enumerate(f):
            try:
                baseName = os.path.basename(line)
                if "json" in baseName:
                    filename = line.strip()     #remove leading space
                    
                    fJson = open(filename, "r")
                    [asksL, bidsL] = parseJson(fJson.readline(), line)
                    fJson.close()
                    
                    if all([len(x) > 0 for x in [asksL, bidsL]]):   #just those with full asks AND bids
                        baseName = os.path.basename(line)
                        eventDate = int(baseName.split(".")[0])
                        sys.stderr.write("%s" % line)
                        
                        for lst in asksL:
                            try:
                                arow["eventDate"] = int(fl.D(eventDate) * fl.ED6)
                                arow["price"] = int(fl.D(lst[0]) * fl.PR5)
                                arow["amount"] = int(fl.D(lst[1]) * fl.AM8)
                                if int(fl.D(lst[1]) * fl.AM8) != 0:  #keep only relevant data
                                    arow.append()
                            except Exception as ex:
                                sys.stderr.write("\naskL: %s" % ex)
                        for lst in bidsL:
                            try:
                                brow["eventDate"] = int(fl.D(eventDate) * fl.ED6)
                                brow["price"] = int(fl.D(lst[0]) * fl.PR5)
                                brow["amount"] = int(fl.D(lst[1]) * fl.AM8)
                                if int(fl.D(lst[1]) * fl.AM8) != 0:  #keep only relevant data
                                    brow.append()
                            except Exception as ex:
                                sys.stderr.write("\naskL: %s" % ex)
                        #we flush for every record otherwise we get performance warning from pyTables  
                        asksT.flush()
                        bidsT.flush()
            except KeyError as ex:
                sys.stderr.write("for: keyError: %s" % ex)
                f.close()
                sys.exit()
            except ValueError as ex:
                sys.stderr.write("valueError: %s" % ex)
                sys.stderr.write("\nfilename: %s" % line)
                continue
            except Exception as ex:
                sys.stderr.write("\nfor loop: %s" % ex)
                sys.stderr.write("\nfilename: %s" % line)
                f.close()
                sys.exit()
            percent = float(f.tell()) / float(file_size)
            #progress status
            sys.stderr.write(" percent: %.6f, %i, %s" % (percent, index, line))
        
            
        f.close()          
        h5file.close()
    except Exception as ex:
        sys.stderr.write("writeStatesFromList: %s" % ex)
        f.close()
        h5file.close()
def buildIndex():
    """
    builds completly sorted index so we can later iterate over it in a sorted manner
    """
    try:
        h5FileName = raw_input("DB name: ")
        groupName = raw_input("group name: ")
        tableToIndex = raw_input("table to index: ")
        db_filters = tables.Filters(complevel=9, complib='blosc', shuffle=1)
        h5file = tables.openFile(h5FileName, filters=db_filters, mode="a")
        
        group = getattr(h5file.root, groupName)
        decisionD = {"asks": getattr(group, "asks"),
                     "bids": getattr(group, "bids"),
                     "trades": getattr(group, "trades"),
                     "events": h5file.root.events}        
        table = decisionD[tableToIndex]
        
        col = tables.Column(table, "eventDate", table.description)  #build index by eventDate column
        sys.stderr.write("\nis %s already indexed? %s" % (col, col.is_indexed))
        if col.is_indexed:
            col.removeIndex()
        sys.stderr.write("\nindexing: %s" % col)
        col.createCSIndex(db_filters, tmp_dir="/run/shm")   #build tmp file index in RAM
        h5file.close()
    except tables.NodeError as ex:
        sys.stderr.write("\nbuildIndex:NodeError: %s " % ex)
    except Exception as ex:
        sys.stderr.write("\nbuildIndex: %s" % ex)
        h5file.close()

def getMasterDB(_db_h5, _dbFilters, _groupName):
    try:
        try:        
            h5file_ = tables.openFile(_db_h5, mode="a", title="Market_data", filters=_dbFilters)
            
            getattr(h5file_.root, _groupName)   #throws AttributeError if attribute doesn't exist
            sys.stderr.write("\ngroup already exists")    
        except AttributeError:
            sys.stderr.write("\ncreating groups")            
            h5file_.createGroup("/", _groupName, _groupName)
        try:
            newGroup = getattr(h5file_.root, _groupName)
            getattr(newGroup, "asks")    #throws AttributeError if attribute doesn't exist
        except AttributeError:
            sys.stderr.write("\ncreating asks table")
            h5file_.createTable(newGroup, "asks", lib.old.tr_market_variables.Depth, "asks_states", filters=_dbFilters, expectedrows=1000000000)
        try:
            getattr(newGroup, "bids")    #throws AttributeError if attribute doesn't exist
        except AttributeError:
            sys.stderr.write("\ncreating bids table")
            h5file_.createTable(newGroup, "bids", lib.old.tr_market_variables.Depth, "bids_states", filters=_dbFilters, expectedrows=1000000000)
        try:
            getattr(h5file_.root, "events")
        except AttributeError:
            sys.stderr.write("\ncreating events table")
            h5file_.createTable(h5file_.root, "events", lib.old.tr_market_variables.Events, "deltas", filters=_dbFilters, expectedrows=200000000)
        try:
            getattr(newGroup, "trades")
        except AttributeError:
            sys.stderr.write("\ncreating trades table")            
            h5file_.createTable(newGroup, "trades", lib.old.tr_market_variables.Trades, "trades", filters=_dbFilters, expectedrows=10000000)
            
        return h5file_
            
    except Exception as ex:
        sys.stderr.write("\ngetMasterDB: %s" % str(ex))
        sys.exit()      
if __name__ == "__main__":
    try:
        """actions"""
        decisionD = {"writeSecond": writeStatesFromList,#(h5file),  #takes a few hours
                     "index": buildIndex,#(h5file, groupName, "asks"),     #since writeDeltas requires indexed column
                     "writeDeltas": writeDeltas,#(h5file, groupName),     #writes events
                     "getTrades": getMtgoxTrades,#(h5file),
                     "stage2": stage2,    #assumes existance of _misc/db/first|second|trades.h5
                     "stage3": stage3}
        
        decisionD[sys.argv[1]]()
        
        
    except Exception as ex:
        sys.stderr.write("\nmain: %s" % str(ex))
        sys.exit()