from tia.trad.tools.sortedDict import SortedDict
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.tools.ipc.naming_conventions as names
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class TargetManager(object):
    """calculating exact margin price for target of _priceI!:
        col items:
            in asks: am1*prH (1 - fee) = got_funds in bids: got_funds/prL (1-fee) = got_items
         condition:  am1 < got_items:
             prL < prH (1-fee1) (1-fee2)
        col funds:
            prH > prL/[(1-fee1)(1-fee2)]
    """
    def __repr__(self): return "%s(%s)" % (self.__class__.__name__, self.__dict__)
    def __init__(self):
        self.targetFunds = SortedDict()
        self.targetItems = SortedDict()
        self.maxItemsInTM = 10    # in items

    def getTMsum(self, side):
        try:
            sum_ = fl.D("0")
            if side == names.bids:
                for amount in self.targetFunds.values(): sum_ += amount
            elif side == names.asks:
                for amount in self.targetItems.values(): sum_ += amount
            else: raise Exception("unknown side: %s" % side)
            return sum_
        except Exception: raise
    def getTargetPriceForBid(self, _initPrice, _fee):
        try: return _initPrice * (1 - _fee)
        except Exception: raise
    def getTargetPriceForAsk(self, _initPrice, _fee):
        try: return _initPrice / ( 1 - _fee)
        except Exception: raise
    def emitMarketQuote(self, _side, _MarketOfChange):
        try:
            logger.debug("targetManager:emitMarketQuote: hi")

            order_ = {}
            decisionD = {names.asks: {"iterTargets": _MarketOfChange.targetManager.targetFunds.iteritems(reverse=True),
                                      "iterOrderbook": _MarketOfChange.orderbook.asks.iteritems(),
                                      "targetPrice": self.getTargetPriceForBid,
                                      "activeOrders": _MarketOfChange.activeOrders.asks},
                         names.bids: {"iterTargets": _MarketOfChange.targetManager.targetItems.iteritems(),
                                      "iterOrderbook": _MarketOfChange.orderbook.bids.iteritems(reverse=True),
                                      "targetPrice": self.getTargetPriceForAsk,
                                      "activeOrders": _MarketOfChange.activeOrders.bids}
            }

            iterTargets = decisionD[_side]["iterTargets"]
            highestTarget = iterTargets.next()

            exchangeRate = _MarketOfChange.exchangeRates[_MarketOfChange.currency]
            fee = _MarketOfChange.account.feeMaker
            fx = highestTarget[0] * exchangeRate
            targetPrice = fx * (fl.D("1") - fee)

            iterOrderbook = decisionD[_side]["iterOrderbook"]
            ordbItem = iterOrderbook.next()
            # check if the top order is our - if there's an order in activeOrders[side] it's at the top of orderbook
            if decisionD[_side]["activeOrders"]: ordbItem = iterOrderbook.next()

            ordbPrice = ordbItem[0]

            price = 0; amount = 0
            targetAmount = highestTarget[1]
            ordbAmount = ordbItem[1]
            while targetPrice > ordbPrice:
                try:
                    #calc prof amount
                    if ordbAmount > targetAmount:
                        price = ordbPrice
                        amount += highestTarget[1]
                        ordbAmount -= highestTarget[1]

                        nextTarget = iterTargets.next()
                        targetAmount = nextTarget[1]
                        # recalc new target price
                        fx = nextTarget[0] * exchangeRate
                        targetPrice = decisionD[_side]["targetPrice"](fx,fee)
                    else:
                        price = ordbPrice
                        amount += ordbAmount
                        newAsk = iterOrderbook.next()
                        ordbAmount = newAsk[1]
                        # get new ask price
                        ordbPrice = newAsk[0]
                except StopIteration: break
                except Exception: raise

            if price: order_[price] = amount
            return order_
        except Exception: raise

    def targetsAmountSum(self, _side, _Market):
        try:
            logger.debug("targetsAmountSum: hi")

            amount = fl.D("0")
            fee = _Market.account.feeMaker

            if _side == names.bids:
                iterTopOrderPrice = _Market.orderbook.bids.iterkeys(reverse=True)
                topOrderPrice = iterTopOrderPrice.next()
                # TM is in USD so convert to USD for comparison
                fx = topOrderPrice * _Market.exchangeRates[_Market.currency]

                for targetItem in _Market.targetManager.targetFunds.iteritems(reverse=True):
                    targetPrice = targetItem[0] * (1 - fee); targetAmount = targetItem[1]
                    if targetPrice > fx: amount += targetAmount * targetPrice   # optimize for items
                    else: break
                # convert funds to items

            elif _side == names.asks:
                iterTopOrderPrice = _Market.orderbook.asks.iterkeys()
                topOrderPrice = iterTopOrderPrice.next()
                fx = topOrderPrice * _Market.exchangeRates[_Market.currency]

                for targetItem in _Market.targetManager.targetItems.iteritems():
                    targetPrice = targetItem[0] / (1 - fee); targetAmount = targetItem[1]
                    if targetPrice < fx: amount += targetAmount     # optimize for funds
                    else: break
            else: raise Exception("unknown side")

            res_ = (topOrderPrice.quantize(_Market.pip), amount.quantize(_Market.pipAmount))
            return res_
        except Exception: raise

    """
    def updateExposure(self, _eventDate, _executedSide, _MarketsD):
        try:
            logger.debug("targetManager:_updateExposure: hi")

            for Market in _MarketsD.values():
                activeOrder = Market.activeOrders.asks if _executedSide == names.asks else Market.activeOrders.bids

                if activeOrder:
                    if activeOrder.targets: pass    # has other targets, leave it alone
                    else:   # has no more targets, update amount from targetManager
                        targetManagerQuote = Market.targetManager.targetsAmountSum(_executedSide, Market)
                        amountSum = targetManagerQuote[1]
                        newAmount = min([amountSum, Market.minAllowableBet, Market.account.getResources(_executedSide, activeOrder.price)])
                        # check if the order has to be replaced
                        if newAmount:
                            if newAmount != activeOrder.amount:
                                Market.sendEvent(event.cancelOrder(activeOrder))
                                newOrder = orders.Order(activeOrder.price, newAmount, _eventDate, Market, _executedSide)
                                newOrder.targets = activeOrder.targets
                                Market.sendEvent(event.placeOrder(newOrder))
                        else:   # newAmount could be 0
                            Market.sendEvent(event.cancelOrder(activeOrder))
        except Exception: raise
    """
    def update(self, _executedAmount, _executedPrice, _executedType, _Market):
        try:
            logger.debug("update: hi")

            # convert to USD
            exchangeRate = _Market.exchangeRates[_Market.currency]
            executedPriceForTM = _executedPrice * exchangeRate

            # eat TM
            # change on the same side in other markets am iff targets=={} and compare to newAm=TMsum
            fee = _Market.account.feeMaker
            position = 0
            foundTarget = 0
            executedSide = _executedType
            keysToDelete = []
            if executedSide == names.bids:
                # check if there are any targets to realize at all in AO
                largestBid = _Market.orderbook.bids.largest_key() * exchangeRate
                targets = _Market.targetManager.targetFunds._sorted_keys    # in USD
                for index, target in enumerate(targets):
                    if target * (1 - fee) > largestBid:
                        foundTarget = 1
                        position = index
                # eat targets
                if foundTarget:
                    for index in xrange(position, len(targets) ):
                        key = targets[index]
                        oldTargetAmount = _Market.targetManager.targetFunds[key]
                        _Market.targetManager.targetFunds[key] -= _executedAmount
                        if _Market.targetManager.targetFunds[key] > 0:
                            _executedAmount = 0
                            break
                        else:
                            keysToDelete.append(key)
                            _executedAmount -= oldTargetAmount
                # delete keys
                for key in keysToDelete: del _Market.targetManager.targetFunds[key]

                # what's left throw in other tm with price*fee
                if _executedAmount:
                    targetPrice = (executedPriceForTM / (1 - fee)).quantize(_Market.pip)
                    try: _Market.targetManager.targetItems[targetPrice] += _executedAmount
                    except KeyError: _Market.targetManager.targetItems[targetPrice] = _executedAmount


            elif executedSide == names.asks:
                # find index
                smallestAsk = _Market.orderbook.asks.smallest_key() * exchangeRate
                targets = _Market.targetManager.targetItems._sorted_keys    # already in USD
                for index, target in enumerate(targets):
                    if target / (1 - fee) < smallestAsk:
                        foundTarget = 1
                        position = index    # greatest of all target
                # eat targets
                if foundTarget:
                    for index in reversed(xrange(position, len(targets) ) ):
                        key = targets[index]
                        oldTargetAmount = _Market.targetManager.targetItems[key]
                        _Market.targetManager.targetItems[key] -= _executedAmount
                        if _Market.targetManager.targetItems[key] > 0:
                            _executedAmount = 0
                            break
                        else:
                            keysToDelete.append(key)
                            _executedAmount -= oldTargetAmount
                # delete keys
                for key in keysToDelete: del _Market.targetManager.targetItems[key]
                # what's left throw in other tm with price*fee
                if _executedAmount:
                    targetPrice = (executedPriceForTM * (1 - fee)).quantize(_Market.pip)
                    try: _Market.targetManager.targetFunds[targetPrice] += _executedAmount
                    except KeyError: _Market.targetManager.targetFunds[targetPrice] = _executedAmount
            else: raise Exception("unknown side: %s" % executedSide)
        except Exception: raise
