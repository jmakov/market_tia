import sys
import json
import tia.configuration as conf

def report(_marketName, _MarketsD):
    try:
        universeSize = 0
        cumulFunds = 0; cumulItems = 0
        cumulVal = 0
        topBids = []
        topAsks = []
        # get universe state
        for Market in _MarketsD.values():
            exchangeRate = Market.exchangeRates[Market.currency]
            if conf.UNIVERSE in Market.name:
                universeSize += 1
                locFunds = Market.account.reservedFunds + Market.account.availableFunds
                cumulFunds += locFunds
                locItems = Market.account.reservedItems + Market.account.availableItems
                cumulItems += locItems
                try:
                    topBid = Market.orderbook.bids.largest_key() * exchangeRate     # for pretty printing of TM status
                    topAsk = Market.orderbook.asks.smallest_key() * exchangeRate
                    cumulVal += locFunds + locItems * Market.orderbook.bids.largest_key()
                except KeyError:
                    cumulVal += locFunds
                    topBid = 0
                    topAsk = 0

                topBids.append(float(topBid))
                topAsks.append(float(topAsk))

        # local state
        Market = _MarketsD[_marketName]
        tmpF = Market.targetManager.targetFunds
        tmpI = Market.targetManager.targetItems
        tmFunds = {}; tmItems = {}
        for p in tmpF: tmFunds[float(p)] = float(tmpF[p])
        for p in tmpI: tmItems[float(p)] = float(tmpI[p])
        #for amount in Market.targetManager.targetFunds.values(): tmFunds += amount
        #for amount in Market.targetManager.targetItems.values(): tmItems += amount
        if conf.UNIVERSE in Market.name:
            try:
                topBid = Market.orderbook.bids.largest_key()    # for loc values
                topAsk = Market.orderbook.asks.smallest_key()
                sys.stderr.write("\nuniverse\tF: %s\tI: %s\tV: %s:L1:%s|%s\tdate: %s" % (cumulFunds, cumulItems, cumulVal, topBid, topAsk, Market.lastUpdate))
                dump = {"cumulFunds": float(cumulFunds), "cumulItems": float(cumulItems), "value": float(cumulVal),
                        "tmFunds": tmFunds, "tmItems": tmItems,
                        "universeSize": universeSize,
                        "topBids": topBids,
                        "topAsks": topAsks}
                with open(conf.FN_REPORT, "a") as f:
                    json.dump(dump, f)
                    f.write("\n")
            except KeyError as ex:
                raise Exception("ex: %s, %s" % (ex, Market))
        else:
            sys.stderr.write("\nmarket not in universe: %s" %_marketName)


    except Exception: raise
