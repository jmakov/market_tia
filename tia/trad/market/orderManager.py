import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.market.orders as orders; reload(orders)
from tia.trad.tools.dicDiff import DictDiff
import tia.trad.market.events as event; reload(event)
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


def cancelOrder(_Order, _ChangedMarket):
    try:
        logger.debug("cancelOrder: hi")

        _ChangedMarket.sendEvent(event.onCancelOrder(names.orderCancel, _ChangedMarket.name, _Order.oid, _Order.type, _Order))
    except Exception: raise
def placeOrder(_Order, _foundTargets, _ChangedMarket):
    try:
        logger.debug("placeOrder: hi")

        price = _Order.price.quantize(_ChangedMarket.pip)
        amount = _Order.amount.quantize(_ChangedMarket.pipAmount)
        dummyOid = 123
        _ChangedMarket.sendEvent(event.onPlaceOrder(names.orderPlace, _ChangedMarket.name, price, amount, _Order.type, _Order.datePlaced, _foundTargets, dummyOid))
    except Exception: raise
def replaceOrder(_Order, _eventDate, _priorityExecutionPrice, _newAmount, _foundTargets, _ChangedMarket):
    try:
        logger.debug("replaceOrder: hi")

        if _Order.price != _priorityExecutionPrice or _Order.amount != _newAmount:
            cancelOrder(_Order, _ChangedMarket)
            newOrder = orders.Order(_ChangedMarket.name, _priorityExecutionPrice, _newAmount, _Order.type, _eventDate)
            placeOrder(newOrder, _foundTargets, _ChangedMarket)
    except Exception: raise
def replaceOnL2Change(_changedSide, _ChangedMarket, _UniverseD):
    # cancel current AO and replace tighter to l2
    try:
        logger.debug("replaceOnL2Change: hi")

        foundTargets = _findTargets(_changedSide, _ChangedMarket, _UniverseD)
        # sim our action instantly so that we can calculate priority price correctly
        #e.g. del out top order
        if _changedSide == names.asks:
            AO = _ChangedMarket.activeOrders.asks
            l1ask = _ChangedMarket.orderbook.asks.smallest_key()
            del _ChangedMarket.orderbook.asks[l1ask]
        elif _changedSide == names.bids:
            AO = _ChangedMarket.activeOrders.bids
            l1bid = _ChangedMarket.orderbook.bids.largest_key()
            del _ChangedMarket.orderbook.bids[l1bid]
        else: raise Exception("unknown side: %s" % _changedSide)

        [newAmount, newPriorityExecutionPrice] = getNewAmount(_changedSide, foundTargets, _ChangedMarket)
        if newAmount:
            replaceOrder(AO, AO.datePlaced, newPriorityExecutionPrice, newAmount, foundTargets, _ChangedMarket)
    except Exception: raise
def getPriorityExecutionPrice(_changedSide, _topOrderPrice, _largestBid, _smallestAsk, _ChangedMarket):
    # don't overbid own orders. if top price the same as ours, replaceOrder will not replace it unless the amount changes
    try:
        logger.debug("getPriorityExecutionPrice: hi")

        pip = _ChangedMarket.pip

        if _changedSide == names.bids:
            priorityExecutionPrice = _topOrderPrice + pip
            AO = _ChangedMarket.activeOrders.bids
            if AO:
                if AO.price == _largestBid:
                    priorityExecutionPrice = AO.price

        elif _changedSide == names.asks:
            priorityExecutionPrice = _topOrderPrice - pip
            AO = _ChangedMarket.activeOrders.asks
            if AO:
                if AO.price == _smallestAsk:
                    priorityExecutionPrice = AO.price
        else: raise Exception("Unknown side: %s" % _changedSide)

        return priorityExecutionPrice
    except Exception: raise



def _findTargets(_changedSide, _ChangedMarket, _UniverseD):
    try:
        logger.debug("_findTargets: hi")

        # recognize opportunity
        foundTargets = {}
        for strategy in _ChangedMarket.activeStrategies.emitsLimitOrder.values():
            retDct = strategy.findExits(_changedSide, _ChangedMarket, _UniverseD)
            foundTargets.update(retDct)
        return foundTargets
    except Exception: raise
def getNewAmount(_changedSide, _foundTargets, _ChangedMarket):
    try:
        logger.debug("getNewAmount: hi")

        # get amount from TM
        targetManagerQuote = _ChangedMarket.targetManager.targetsAmountSum(_changedSide, _ChangedMarket)
        topOrderPrice = targetManagerQuote[0]
        # get a price for priority execution on curr market
        onePip = _ChangedMarket.pip; smallestAsk = _ChangedMarket.orderbook.asks.smallest_key(); largestBid = _ChangedMarket.orderbook.bids.largest_key()

        # if our order at the top, priorityPrice == same as top, so it will get replaced only if amount changes
        if smallestAsk - largestBid > onePip:
            priorityExecutionPrice = getPriorityExecutionPrice(_changedSide, topOrderPrice, largestBid, smallestAsk, _ChangedMarket)
        else:
            priorityExecutionPrice = largestBid if _changedSide == names.bids else smallestAsk

        # get amount for curr market from TM
        availItems = _ChangedMarket.account.getResources(_changedSide, priorityExecutionPrice, _ChangedMarket)     # all amounts are in items!

        maxSaturationAtSomePrice = 2
        TMamountSum = targetManagerQuote[1]
        # if in bids, change returned funds into items
        if _changedSide == names.bids: TMamountSum = (TMamountSum / priorityExecutionPrice).quantize(_ChangedMarket.pip)

        # keep always some funds to realize TM
        if availItems <= _ChangedMarket.minAllowableBet: newAmount = fl.D("0")
        # keep some funds to realize TM
        # TM saturation: wait for TM to realize this amount first
        #elif amountSum > maxSaturationAtSomePrice: newAmount = min([amountSum, availItems])
        # place a bet in addition to TM realization amount
        else:
            minBet = fl.D("1") if availItems > 10 else fl.D("0.1")
            foundExitsBet = min([TMamountSum + minBet, availItems])
            newAmount = foundExitsBet if _foundTargets else min([TMamountSum, availItems])

        logger.debug("newAm: %s, priorityPrice: %s" % (newAmount, priorityExecutionPrice))
        if newAmount <= _ChangedMarket.minAllowableBet: newAmount = 0
        return [newAmount, priorityExecutionPrice]
    except Exception: raise

def getAmountForMarketOrder(_changedSide, _ChangedMarket):
    try:
        logger.debug("getAmountForMarketOrder: hi")
        res_ = {"newAmount": None, "changedSideAO": None}

        exchangeRate = _ChangedMarket.exchangeRates[_ChangedMarket.currency]
        targetSide = names.bids if _changedSide == names.asks else names.asks

        if _changedSide == names.bids:
            AO = _ChangedMarket.activeOrders.bids
            opposedAO = _ChangedMarket.activeOrders.asks
            # try to realize items
            try:
                target = _ChangedMarket.targetManager.targetItems.smallest_item()
                targetPrice = target[0]
                targetAmount = target[1]
            except KeyError: target = None

            if target:
                topOrderIter = _ChangedMarket.orderbook.bids.iteritems(reverse=True)
                topOrder = topOrderIter.next(); topOrderPrice = topOrder[0]; topOrderAmount = topOrder[1]
                # check if top order ours
                try:
                    if AO.price == topOrderPrice:
                        topOrder = topOrderIter.next(); topOrderPrice = topOrder[0]; topOrderAmount = topOrder[1]
                        res_["changedSideAO"] = AO

                    # AO might be behind top order so check the top order
                except AttributeError: pass     #AO might not exist
                finally:
                    # check if it crosses
                    if topOrderPrice * exchangeRate + 10 * _ChangedMarket.pip > targetPrice:
                        logger.debug("topOrderUSD:%s, target:%s" % (topOrderPrice * exchangeRate, targetPrice))

                        availItems = _ChangedMarket.account.getResources(targetSide, topOrderPrice, _ChangedMarket)
                        amount = min([targetAmount, topOrderAmount, availItems])
                        res_["newAmount"] = amount
                        res_["priorityExecutionPrice"] = topOrderPrice
                        res_["side"] = targetSide
                        res_["oppositeAO"] = opposedAO

        elif _changedSide == names.asks:
            AO = _ChangedMarket.activeOrders.asks
            opposedAO = _ChangedMarket.activeOrders.bids
            # try to realize funds
            try:
                target = _ChangedMarket.targetManager.targetFunds.largest_item()
                targetPrice = target[0]
                targetAmount = target[1]
            except KeyError: target = None

            if target:
                topOrderIter = _ChangedMarket.orderbook.asks.iteritems()
                topOrder = topOrderIter.next(); topOrderPrice = topOrder[0]; topOrderAmount = topOrder[1]
                # check if top order ours
                try:
                    if AO.price == topOrderPrice:
                        topOrder = topOrderIter.next(); topOrderPrice = topOrder[0]; topOrderAmount = topOrder[1]
                        res_["changedSideAO"] = AO
                except AttributeError: pass
                finally:
                    # check if it crosses
                    if topOrderPrice * exchangeRate - 10 * _ChangedMarket.pip < targetPrice:
                        logger.debug("topOrderUSD:%s, target:%s" % (topOrderPrice * exchangeRate, targetPrice))

                        availItems = _ChangedMarket.account.getResources(targetSide, topOrderPrice, _ChangedMarket)
                        amount = min([targetAmount, topOrderAmount, availItems])
                        res_["newAmount"] = amount
                        res_["priorityExecutionPrice"] = topOrderPrice
                        res_["side"] = targetSide
                        res_["oppositeAO"] = opposedAO

        else: raise Exception("unknown side: %s" % _changedSide)

        logger.debug("getAmountForMarketOrder: %s" % res_)
        return res_
    except Exception: raise

def _manageCurrentMarket(_eventDate, _changedSide, _foundTargets, _ChangedMarket):
    try:
        logger.debug("_manageCurrentMarket: hi")
        [newAmount, priorityExecutionPrice] = getNewAmount(_changedSide, _foundTargets, _ChangedMarket)
        # check if we have to alter order already in the market
        activeOrder = _ChangedMarket.activeOrders.bids if _changedSide == names.bids else _ChangedMarket.activeOrders.asks
        logger.debug("activeOrder: %s" % activeOrder)

        if activeOrder:
            if newAmount:
                replaceOrder(activeOrder, _eventDate, priorityExecutionPrice, newAmount, _foundTargets, _ChangedMarket)
            # newAmount can be 0
            else: cancelOrder(activeOrder, _ChangedMarket)
        else:
            if newAmount:
                newOrder = orders.Order(_ChangedMarket.name, priorityExecutionPrice, newAmount, _changedSide, _eventDate)
                placeOrder(newOrder, _foundTargets, _ChangedMarket)

    except Exception: raise

def _manageFoundTargets(_eventDate, _changedSide, _foundTargets, _ChangedMarket, _UniverseD):
    """
    manage exposure: expose or change activeOrder
    """
    try:
        logger.debug("_manageFoundTargets: hi")

        targetSide = names.asks if _changedSide == names.bids else names.bids

        for targetMarketname in _foundTargets:
            TargetMarket = _UniverseD[targetMarketname]
            if TargetMarket != _ChangedMarket:  # since for our market we already manage it
                _manageCurrentMarket(_eventDate, targetSide, _foundTargets, TargetMarket)
    except Exception: raise

def _manageLostTargets(_eventDate, _changedSide, _foundTargets, _ChangedMarket, _UniverseD):
    try:
        logger.debug("_manageLostTargets: hi")

        #HANDLE MARKETS WITH NO SIGNAL
        # check markets with no signal: if an order sees still a target after sgn loss from our market, leave it
        # else get amount from targetManager
        lostSignal = DictDiff(_foundTargets, _UniverseD).removed()
        for marketName in lostSignal:
            LostMarket = _UniverseD[marketName]

            if LostMarket != _ChangedMarket:    # since changedMarket is already handeled
                if _changedSide == names.bids:
                    activeOrder = LostMarket.activeOrders.asks
                    targetSide = names.asks
                elif _changedSide == names.asks:
                    activeOrder = LostMarket.activeOrders.bids
                    targetSide = names.bids
                else: raise Exception("unknown side: %s" % _changedSide)

                if activeOrder:
                    # remove our market from targets since no more signal
                    try: del activeOrder.targets[_ChangedMarket.name]
                    except KeyError: pass

                    if activeOrder.targets: pass    # has other targets, leave it alone
                    else:   # has no more targets -> no bets, just empty TM:  get amount from targetManager
                        targetManagerQuote = LostMarket.targetManager.targetsAmountSum(targetSide, LostMarket)
                        amountSum = targetManagerQuote[1]
                        newAmount = min([amountSum, LostMarket.minAllowableBet, LostMarket.account.getResources(targetSide, activeOrder.price, LostMarket)])
                        # check if the order has to be replaced
                        if newAmount: replaceOrder(activeOrder, _eventDate, activeOrder.price, newAmount, {}, LostMarket)
                        else: cancelOrder(activeOrder, LostMarket)
    except Exception: raise



def manageUniverse(_eventDate, _ChangedMarket, _UniverseD):
    try:
        logger.debug("\nmanageUniverse: hi")

        """
        #check if TM can try to empty itself with a market order
        marketQuote = _MarketOfChange.targetManager.emitMarketQuote(side, _MarketOfChange)
        if marketQuote:
            pass
            #marketOrderProcedure -
        else:
            #TODO handle marketOrder Strategies
            # recognize opportunity for a market order
            #for strategy in _MarketOfChange.activeStrategies.emitsMarketOrder:
            #    marketQuote = strategy.returnMarketQuote(side, _MarketOfChange, _MarketsOtherD)
            # get marketQuote
            if marketQuote:
                pass
                #marketOrderProcedure -
            # recognize opportunity for limit order in this Market
            else:
        """


        l1askChanged = _ChangedMarket.filters.minAskChanged; l1bidChanged = _ChangedMarket.filters.maxBidChanged

        AOA = _ChangedMarket.activeOrders.asks; AOB = _ChangedMarket.activeOrders.bids
        l2askChanged = AOA and _ChangedMarket.filters.secondaryAskChanged
        l2bidChanged = AOB and _ChangedMarket.filters.secondaryBidChanged
        l1b = _ChangedMarket.orderbook.bids.largest_item()
        l1a = _ChangedMarket.orderbook.asks.smallest_item()

        L1 = "L1changed"; L2 = "L2changed"; ActiveOrder = "ActiveOrder"; topOrder = "topOrder"
        d = {names.bids: {L1: l1bidChanged, L2: l2bidChanged, ActiveOrder: AOB, topOrder: l1b},
             names.asks: {L1: l1askChanged, L2: l2askChanged, ActiveOrder: AOA, topOrder: l1a}
            }

        # check for market orders
        foundMO = 0
        """
        for changedSide in d:
            marketOrderD = getAmountForMarketOrder(changedSide, _ChangedMarket)
            amount = marketOrderD["newAmount"]
            if amount > _ChangedMarket.minAllowableBet:
                foundMO = 1
                changedSideAO = marketOrderD["changedSideAO"]
                if changedSideAO: cancelOrder(changedSideAO, _ChangedMarket)

                newOrder = orders.Order(_ChangedMarket.name, marketOrderD["priorityExecutionPrice"], marketOrderD["newAmount"], marketOrderD["side"], _eventDate)
                AO = marketOrderD["oppositeAO"]
                if AO: cancelOrder(AO, _ChangedMarket)
                placeOrder(newOrder, {}, _ChangedMarket)
        # limit orders and new bets
        """
        if foundMO: pass
        else:
            for changedSide in d:
                container = d[changedSide]
                # if not tight above any more, request reconfiguration
                if container[L2]:
                    AO = container[ActiveOrder]
                    L1OrderAmount = container[topOrder][1]
                    if L1OrderAmount <= AO.amount:  # since if greater sbdy else has placed an order at that price
                        replaceOnL2Change(changedSide, _ChangedMarket, _UniverseD)
                else:
                    foundTargets = _findTargets(changedSide, _ChangedMarket, _UniverseD)

                    _manageCurrentMarket(_eventDate, changedSide, foundTargets, _ChangedMarket)

                    _manageFoundTargets(_eventDate, changedSide, foundTargets, _ChangedMarket, _UniverseD)

                    _manageLostTargets(_eventDate, changedSide, foundTargets, _ChangedMarket, _UniverseD)

    except Exception: raise
