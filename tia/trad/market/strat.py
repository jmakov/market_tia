import tia.trad.tools.ipc.naming_conventions as names
import tia.configuration as conf
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class ActiveStrategies(object):
    def __init__(self):
        self.emitsMarketOrder = {}
        self.emitsLimitOrder = {}
    def registerStrategy(self, _Strategy):
        try:
            logger.debug("registerStrategy: hi")

            if _Strategy.emitsOrderType == names.marketOrder:
                self.emitsMarketOrder[_Strategy] = _Strategy()
            elif _Strategy.emitsOrderType == names.limitOrder:
                self.emitsLimitOrder[_Strategy] = _Strategy()
            else: raise Exception("unknown emitsOrderType")
        except Exception: raise


class Arbitrage(object):
    emitsOrderType = names.marketOrder
    def returnMarketQuote(self, _side, _MarketOfChange, _MarketsOtherD):
        pass

class MarketMaker(object):
    emitsOrderType = names.limitOrder
    def findExits(self, _side, _Market, _MarketsD):
        """calculating exact margin price for target of _priceI!:
        col items:
            in asks: am1*prH (1 - fee) = got_funds in bids: got_funds/prL (1-fee) = got_items
         condition:  am1 < got_items:
             prL < prH (1-fee1) (1-fee2)
        col funds:
            prH > prL/[(1-fee1)(1-fee2)]
        """
        try:
            exits_ = {}

            localFee = _Market.account.feeMaker
            exchangeRate = _Market.exchangeRates[_Market.currency]

            if _side == names.bids:
                largestBid = _Market.orderbook.bids.largest_key() * exchangeRate
                for TargetMarket in _MarketsD.values():
                    if conf.UNIVERSE in TargetMarket.name:
                        if TargetMarket.filters.gotOrderbook:
                            targetPrice = largestBid / ((1 - localFee) * (1 - TargetMarket.account.feeMaker))
                            if targetPrice < TargetMarket.orderbook.asks.smallest_key() * TargetMarket.exchangeRates[TargetMarket.currency]:
                                exits_[TargetMarket.name] = None

            elif _side == names.asks:
                smallestAsk = _Market.orderbook.asks.smallest_key() * exchangeRate
                for TargetMarket in _MarketsD.values():
                    if conf.UNIVERSE in TargetMarket.name:
                        if TargetMarket.filters.gotOrderbook:
                            targetPrice = smallestAsk * (1 - localFee) * (1 - TargetMarket.account.feeMaker)
                            if targetPrice > TargetMarket.orderbook.bids.largest_key() * TargetMarket.exchangeRates[TargetMarket.currency]:
                                exits_[TargetMarket.name] = None
            return exits_
        except Exception: raise
class MarketMakerEater(object):
    emitsOrderType = names.limitOrder
    def findExits(self, _side, _Market, _MarketsD):
        try:
            return {_Market.name: _Market}
        except Exception: raise









def getTargetPrice(_HeadPrice, _HeadType, _HeadMarket, _TargetMarket):
    """calculating exact margin price for target of _priceI!:
    col items:
        in asks: am1*prH (1 - fee) = got_funds in bids: got_funds/prL (1-fee) = got_items
     condition:  am1 < got_items:
         prL < prH (1-fee1) (1-fee2)
    col funds:
        prH > prL/[(1-fee1)(1-fee2)]
    """
    try:
        logger.debug("getTargetPrice: hi")
        assert type(_HeadPrice) == int, _HeadPrice
        assert _HeadType in (IPC.asks, IPC.bids), _HeadType

        marginPrice_ = None
        # multiplicator can be 0 in markets with fee = 0
        multiplicator = Mia.mul(10**7 - _HeadMarket.account.feeMaker, 7, 10**7 - _TargetMarket.account.feeMaker, 7, 5)
        if _HeadType == IPC.bids:
            if multiplicator == 0: marginPrice_ = _HeadPrice + onePip(_TargetMarket)
            else: marginPrice_ = Mia.div(_HeadPrice, 5, multiplicator, 5, 5)
        elif _HeadType == IPC.asks:
            if multiplicator == 0: marginPrice_ = _HeadPrice - onePip(_TargetMarket)
            else: marginPrice_ = Mia.mul(_HeadPrice, 5, multiplicator, 5, 5)
        return marginPrice_
    except Exception: raise
def bestPriceBehindMargin(_marginPrice, _orderType, _Market):
    """
    if order.type = ask, returns best price in asks
    :param _marginPrice:
    :param _Market:
    :return:
    """
    try:
        logger.debug("bestPriceBehindMargin: hi")
        assert type(_marginPrice) == int, _marginPrice
        assert _orderType in (IPC.asks, IPC.bids), _orderType

        if _orderType not in [IPC.bids, IPC.asks]: raise AssertionError("Unexpected arg", _orderType)

        bestPrice_ = None
        if _orderType == IPC.bids:
            for bid in _Market.orderbook.bids.iterkeys(reverse=True):
                if bid < _marginPrice:
                    bestPrice_ = bid + onePip(_Market)
                    break
        elif _orderType == IPC.asks:
            for ask in _Market.orderbook.asks.iterkeys():
                if ask > _marginPrice:
                    bestPrice_ = ask - onePip(_Market)
                    break
        if bestPrice_ is None:
            raise Exception("bestPrice = None. MarginPrice = %s, OrderType = %s, market = %s", [_marginPrice, _orderType, _Market.__class__.__name__])
        return bestPrice_
    except Exception: raise
def findHeads(_Market):
    try:
        heads_ = []
        for Site in [_Market.activeOrders.bids, _Market.activeOrders.asks]:
            for activeOrderPrice in Site:
                for oid in Site[activeOrderPrice]:
                    ActiveOrder = Site[activeOrderPrice][oid]
                    if ActiveOrder.head is None: heads_.append(ActiveOrder)
        # max 2 heads in one market
        if len(heads_) > 2: raise Exception("Multiple heads: %s", len(heads_))
        return heads_
    except Exception: raise

def _suppRepositionHeadsComparisonAsks(_targetPrice, _marginP):
    try: return _targetPrice < _marginP
    except Exception: raise
def _suppRepositionHeadsComparisonBids(_targetPrice, _marginP):
    try: return _targetPrice > _marginP
    except Exception: raise
def repositionHeads(_mObjD, _Market, _heads):
    """
    #@attention target placement is managed by targetManager
    :param _mObjD:
    :param _Market: marketObj where new delta updated orderbook
    :return:
    """
    try:
        logger.debug("repositionHeads: %s: hi", _Market.__class__.__name__)

        # config
        decisionD = {IPC.asks: [feature.pMinAsk, feature.pMinAsk(_Market), IPC.orderSell,
                               _suppRepositionHeadsComparisonAsks],
                     IPC.bids: [feature.pMaxBid, feature.pMaxBid(_Market), IPC.orderBuy,
                               _suppRepositionHeadsComparisonBids]}
        # repositioning of heads
        for Order in _heads:
            # check if repositioning needed
            pp = decisionD[Order.type][0](_Market)
            absDiff = abs(pp - Order.price)
            statement = absDiff > onePip(_Market)
            #oldExpr = abs(decisionD[Order.type][0](_Market) - Order.price) > onePip(_Market)
            if statement:
                logger.debug("repositionHeads: reposition head")

                # cancel old order
                Order.cancel(_Market)
                # construct new, adjusted order
                date = Mm.getTime()
                headNewPrice = decisionD[Order.type][1]
                adjustedOrder = Mo.Order(headNewPrice, Order.amount, date, _Market.__class__.__name__,
                                         decisionD[Order.type][2], Order.type)
                # adjust targets
                headExpectedReturn = _Market.account.transaction(Order)
                for Target in Order.targets:
                    logger.debug("repositionHeads: reposition target")

                    targetMarket = _mObjD[Target.market]
                    marginPr = getTargetPrice(adjustedOrder.price, adjustedOrder.type, targetMarket)    # diff fees in diff markets
                    # check if target needed to be adjusted
                    if decisionD[Order.type][3](Target.price, marginPr):
                        #if not, add old target to new order's.targets
                        adjustedOrder.targets.append(Target)
                    else:
                        #FIXME: what would you cancel if ther's nothing? build ord mngmnt!
                        # cancel old order
                        Target.cancel(targetMarket)
                        # construct new target
                        targetPrice = bestPriceBehindMargin(marginPr, Target.type, targetMarket)
                        adjustedTarget = Mo.Order(targetPrice, headExpectedReturn,
                                                  _datePlaced=date, _market=targetMarket.__class__.__name__,
                                                  _action=decisionD[Order.type][2], _type=Target.type, _head=adjustedOrder)
                        # append new targets to head
                        adjustedOrder.targets.append(adjustedTarget)
                # place new head
                _Market.ordersQueue.append(adjustedOrder)
    except Exception: raise
#TODO: refactoring - order management
def constructTargets(): pass
def constructHead(_mObjD, _Market, _typeOfHead):
    #@attention target placement is managed by targetManager
    try:
        logger.debug("constructHead: hi")
        assert _typeOfHead in (IPC.asks, IPC.bids), ("Unexpected type", _typeOfHead)

        date = Mm.getTime()
        #TODO: remove debug stuff
        maxBid = _Market.orderbook.bids._orderbook.largest_key()
        minAsk = _Market.orderbook.asks._orderbook.smallest_key()

        targetTypeD = {IPC.asks: IPC.bids, IPC.bids: IPC.asks}
        decisionD = {IPC.asks: [feature.pMinAsk(_Market), IPC.orderSell, _Market.account.betMinItems],
                     IPC.bids: [feature.pMaxBid(_Market), IPC.orderBuy, _Market.account.betMinFunds]}
        # construct new head
        # don't trigger a market order
        newHead = None
        if feature.pSpread(_Market) > 2 * onePip(_Market):
            # construct new head
            headPrice = decisionD[_typeOfHead][0]
            headInvestment = decisionD[_typeOfHead][2]
            newHead = Mo.Order(headPrice, headInvestment, date,  _Market.__class__.__name__,
                               decisionD[_typeOfHead][1], _typeOfHead)
            # construct new target
            headExpectedReturn = _Market.account.transaction(newHead)   # items or funds
            targetMarket = _Market
            marginPr = getTargetPrice(headPrice, _typeOfHead, targetMarket)
            targetType = targetTypeD[_typeOfHead]
            targetPrice = bestPriceBehindMargin(marginPr, targetType, targetMarket)
            newTarget = Mo.Order(targetPrice,  headExpectedReturn, date, _Market.__class__.__name__,
                                 decisionD[targetType][1], targetType, _head=newHead)
            # make head aware of new target
            newHead.targets.append(newTarget)
            # place head
            _Market.ordersQueue.append(newHead)
        return newHead
    except Exception: raise
def placeHeads(_mObjD, _Market, _heads_):
    try:
        logger.debug("placeHead: hi")

        targetHeadTypeD = {IPC.asks: IPC.bids, IPC.bids: IPC.asks}
        # if no heads, place heads
        if len(_heads_) == 0:
            for item in [IPC.bids, IPC.asks]:
                head = constructHead(_mObjD, _Market, item)
                if head is not None: _heads_.append(head)
        elif len(_heads_) == 1:
            head = constructHead(_mObjD, _Market, targetHeadTypeD[_heads_[0].type])
            if head is not None: _heads_.append(head)
        else: raise AssertionError("Unhandled param:", len(_heads_))
        return _heads_
    except Exception: raise
