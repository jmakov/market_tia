import heapq
import collections
import ujson as json
import tia.trad.tools.ipc.naming_conventions as names
import linecache
import os
import tia.trad.market.orderbook as ordb
import tia.configuration as conf


def countFileLines(_fileName):
    try:
        f = open(_fileName)
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.read # loop optimization

        buf = read_f(buf_size)
        while buf:
            lines += buf.count('\n')
            buf = read_f(buf_size)

        return lines
    except Exception: raise


#TODO filter out lines with same date e.g. duplicates
if __name__ == "__main__":
    try:
        # unpack batchTrades-> write a file w/o batchTrades:
        oldEvents = open(conf.FN_DB, "r")
        tmpEvents = open("db/tmp", "w")
        sortedEvents = open(conf.FN_DB_SIM, "w")

        print "noLines in %s: %s" % (conf.FN_DB, num_lines)

        print "writing events w/o batchTrades and unpacking"
        sortedDates = []
        #remember line numbers of batchTrade lines
        index = 1
        try:
            for line in oldEvents:
                Event = json.loads(line)
                # unpack onBatchTrades to onTrade
                if Event["name"] == names.onBatchTrades:
                    # unpack
                    for trade in Event["container"]:
                        trade["name"] = names.onTrade
                        trade["market"] = Event["market"]
                        trade["date"] = trade["tid"] if "mtgox" in trade["market"] else int(trade["date"]) * 10**6
                        json.dump(trade, tmpEvents)
                        tmpEvents.write("\n")
                        heapq.heappush(sortedDates, (Event["date"], index))
                        index += 1
                    continue
                # unify dates
                elif Event["name"] == names.onTrade and "mtgox" in Event["market"]:
                    Event["date"] = Event["tid"]
                # optimize sim time by reducing ordb to top 20
                elif Event["name"] == names.onOrderbook:
                    # load orderbook
                    orderbook = ordb.SortedOrderbook(Event["orderbook"])
                    top20Orderbook = {names.asks: [], names.bids: []}

                    bindex = 0
                    for tpl in orderbook.bids.iteritems(reverse=True):
                        if bindex > 20: break
                        top20Orderbook[names.bids].append([str(tpl[0]), str(tpl[1])])
                        bindex += 1
                    aindex = 0
                    for tpl in orderbook.asks.iteritems():
                        if aindex > 20: break
                        top20Orderbook[names.asks].append([str(tpl[0]), str(tpl[1])])
                        aindex += 1

                    Event["orderbook"] = top20Orderbook

                #tmpEvents.write(line)
                json.dump(Event, tmpEvents)
                tmpEvents.write("\n")
                heapq.heappush(sortedDates, (Event["date"], index))
                index += 1
        except ValueError:
            print "malformed line at ", index
        tmpEvents.close()
        oldEvents.close()

        print "writing sorted file"
        while 1:
            try:
                lineNumber = heapq.heappop(sortedDates)[1]
                line = linecache.getline("db/tmp", lineNumber)
                sortedEvents.write(line)
            except IndexError:
                print "no more lines"
                break
            except Exception: raise
        sortedEvents.close()

        #remove tmp file
        #os.remove("db/tmp")
    except Exception: raise