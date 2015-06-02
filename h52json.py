import tables
import tia.trad.market.events as events
import tia.trad.tools.arithm.floatArithm as fl
import ujson
import json
import heapq
import linecache
import os


try:
    h5file = tables.openFile("db/s.h5", filters=tables.Filters(complevel=9, complib='blosc', shuffle=1))
    # all events on random
    tmpFilePath = "db/tmp.json"
    tmpF = open(tmpFilePath, "w")
    # sorted events by date
    eventsF = open("db/events.json", "w")

    rowIndex = 0
    sortedDates = []

    for group in h5file.walkGroups():
        print group
        if group != h5file.root:
            marketName = group._v_name

            # serialize bids and asks to json
            print "serializing deltas"
            for deltas in [group.bids, group.asks]:
                prevDate = deltas[0]["date"]
                side = deltas._v_name

                container = []
                for delta in deltas:
                    date = delta["date"]
                    price = fl.D( str( delta["price"]) )/ 10**5
                    amount = fl.D( str( delta["amount"]) ) / 10**8

                    deltasD = {"market": marketName, "date": date, "price": price, "amount": amount, "type": side}

                    if date != prevDate:
                        if len(container) > 1: Event = events.onBatchDeltas(**{"market": marketName, "date": prevDate, "container": container})
                        else: Event = events.onDelta(**container[0])
                        # book keeping
                        rowIndex += 1
                        heapq.heappush(sortedDates, (date, rowIndex))
                        container = []

                        # write to tmp file
                        dump = Event.serialize()
                        ujson.dump(dump, tmpF)
                        tmpF.write("\n")
                    container.append(deltasD)
                    prevDate = date

            # serialize checkpoints
            print "serializing checkpoints"
            ordbDate = group.cpA[0]["date"]
            currentRowIndex = 0
            while 1: #currentRowIndex != group.cpA.nrows:
                try:
                    orderbook = {"asks": [], "bids": []}
                    eventD = {"market": marketName, "date": ordbDate, "orderbook": orderbook}

                    maxIndexWithCurrentDate = 0
                    for row in group.cpA.where("date == ordbDate"):
                        price = fl.D( str(row["price"]) ) / 10**5
                        amount = fl.D( str(row["amount"]) ) / 10**8
                        orderbook["asks"].append([price, amount])
                        if row.nrow > maxIndexWithCurrentDate:
                            maxIndexWithCurrentDate = row.nrow
                    for row in group.cpB.where("date == ordbDate"):
                        price = fl.D( str(row["price"]) ) / 10**5
                        amount = fl.D( str(row["amount"]) ) / 10**8
                        orderbook["bids"].append([price, amount])


                    Event = events.onOrderbook(**eventD)
                    # check orderbook integrity
                    sask = Event.orderbook.asks.smallest_key()
                    lbid = Event.orderbook.bids.largest_key()
                    assert sask > lbid, (lbid, sask)
                    # book keeping
                    rowIndex += 1
                    heapq.heappush(sortedDates, (ordbDate, rowIndex))
                    # write to tmp file
                    dump = Event.serialize()
                    json.dump(dump, tmpF)   #ujson throws overflow error for some reason
                    tmpF.write("\n")

                    # find next date
                    ordbDate = group.cpA[maxIndexWithCurrentDate + 1]["date"]   # throws an exception when no more rows
                except Exception: break

            # serialize trades
            print "serializing trades"
            prevDate = group.trades[0]["date"]
            container = []
            for row in group.trades:
                price = fl.D( str(row["price"])) / 10**5
                amount = fl.D( str(row["amount"])) / 10**8
                date = row["date"]
                tid = row["tid"]
                tradesD = {"market": marketName, "date": date, "price": price, "amount": amount, "tid": tid}

                if date != prevDate:
                    if len(container) > 1: Event = events.onBatchTrades(**{"market": marketName, "date": prevDate, "container": container})
                    else: Event = events.onTrade(**container[0])
                    # book keeping
                    rowIndex += 1
                    heapq.heappush(sortedDates, (prevDate, rowIndex))
                    container = []

                    # write to tmp file
                    dump = Event.serialize()
                    json.dump(dump, tmpF)   #ujson throws overflow error for some reason
                    tmpF.write("\n")
                container.append(tradesD)
                prevDate = date
    tmpF.close()

    # sort events by date
    print "sorting by date"
    while 1:
        try:
            lineNumber = heapq.heappop(sortedDates)[1]
            line = linecache.getline("db/tmp.json", lineNumber)
            eventsF.writelines(line)
        except IndexError: break
        except Exception: raise

    eventsF.close()

    # delete tmp file
    os.remove(tmpFilePath)
except Exception: raise