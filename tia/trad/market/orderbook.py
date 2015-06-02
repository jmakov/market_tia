import logging
from tia.trad.tools.sortedDict import SortedDict
import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.market.stats as stats
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)



class ActiveOrders(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, _Account):
        self.account = _Account
        self.bids = None #Order()
        self.asks = None #Order()

    def place(self, _Order):
        try:
            assert _Order.__class__.__name__ == "Order", _Order
            logger.debug("%s:PlaceOrder:%s:%s, "
                         "F: %s|%s, I: %s|%s"  % ( _Order.type, _Order.price, _Order.amount,
                                                  self.account.availableFunds, self.account.reservedFunds,
                                                  self.account.availableItems, self.account.reservedItems))

            if _Order.type == names.bids:
                # update AO
                assert self.bids == None, self.bids
                self.bids = _Order
                # update account
                self.account.availableFunds -= _Order.value
                self.account.reservedFunds += _Order.value

            elif _Order.type == names.asks:
                assert self.asks == None, self.asks
                self.asks = _Order
                self.account.availableItems -= _Order.amount
                self.account.reservedItems += _Order.amount
            else: raise Exception("unknown side: %s" % _Order.type)

            logger.debug("acc status: AF:%s|RF:%s, AI:%s, RI:%s" % (self.account.availableFunds, self.account.reservedFunds, self.account.availableItems, self.account.reservedItems))
        except Exception: raise

    def cancel(self, _Order):
        try:
            assert _Order.__class__.__name__ == "Order", _Order
            logger.debug("%s:%s:CancelledOrder:%s:%s, "
                         "F: %s|%s, I: %s|%s"  % (_Order.market, _Order.type, _Order.price, _Order.amount,
                                                  self.account.availableFunds, self.account.reservedFunds,
                                                  self.account.availableItems, self.account.reservedItems))
            if _Order.type == names.bids:
                # update AO
                assert self.bids != None, self.bids
                self.bids = None    # cancelling order
                # update account
                self.account.availableFunds += self.account.reservedFunds
                self.account.reservedFunds = fl.D("0")

            elif _Order.type == names.asks:
                assert self.asks != None, self.asks
                self.asks = None    # cancelling order
                # update account
                self.account.availableItems += self.account.reservedItems
                self.account.reservedItems = fl.D("0")
            else: raise Exception("unknown side: %s" % _Order.type)

            logger.debug("acc status: AF:%s|RF:%s, AI:%s, RI:%s" % (self.account.availableFunds, self.account.reservedFunds, self.account.availableItems, self.account.reservedItems))
        except Exception: raise

    def tradedAmount(self, _executedAmount, _AO, _MarketsD):
        # report transaction
        try:
            logger.debug("orderbook:tradedAmount:%s %s|%s:%s" % (_AO.market, _AO.type, _AO.price, _executedAmount))
            logger.debug("acc status: AF:%s|RF:%s, AI:%s, RI:%s" % (self.account.availableFunds, self.account.reservedFunds, self.account.availableItems, self.account.reservedItems))

            if _AO.type == names.bids:
                # update order
                self.bids.amount -= _executedAmount
                if self.bids.amount == 0:
                    self.bids = None
                    self.account.reservedFunds = 0
                else: self.account.reservedFunds -= (_executedAmount * _AO.price).quantize(fl.QP)

                self.account.availableItems += (_executedAmount * (1 - self.account.feeMaker)).quantize(fl.QA)

            elif _AO.type == names.asks:
                self.asks.amount -= _executedAmount
                if self.asks.amount == 0:
                    self.asks = None
                    self.account.reservedItems = 0
                else: self.account.reservedItems -= _executedAmount.quantize(fl.QA)

                self.account.availableFunds += (_executedAmount * _AO.price * (1 - self.account.feeMaker)).quantize(fl.QP)

            logger.debug("acc status: AF:%s|RF:%s, AI:%s, RI:%s" % (self.account.availableFunds, self.account.reservedFunds, self.account.availableItems, self.account.reservedItems))
            # report update
            stats.report(_AO.market, _MarketsD)
        except Exception: raise


class SortedOrderbook(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, _orderbook=None):
        #_orderbook = {"asks": [["price", "amount"], ...]}
        try:
            self.bids = SortedDict()
            self.asks = SortedDict()
            if _orderbook:
                decisionD = {names.asks: self.asks, names.bids: self.bids}
                for side in decisionD:
                    # check that _orderbook is not empty
                    assert _orderbook[side][0][0], _orderbook

                    for tpl in _orderbook[side]:
                        try: decisionD[side][fl.D(tpl[0])] += fl.D(tpl[1])
                        except KeyError: decisionD[side][fl.D(tpl[0])] = fl.D(tpl[1])
        except Exception: raise
    def simPlace(self, _Order):
        try:
            orderbook = self.bids if _Order.type == names.bids else self.asks

            try: orderbook[_Order.price] = _Order.amount
            except KeyError: orderbook[_Order.price] = _Order.amount
        except Exception: raise
    def simCancel(self, _Order):
        try:
            orderbook = self.bids if _Order.type == names.bids else self.asks
            try:
                orderbook[_Order.price] -= _Order.amount
                if orderbook[_Order.price] <= 0: del orderbook[_Order.price]
            except KeyError: pass   # could have been deleted because of the execution on trade
        except Exception: raise
    def serialize(self):
        try:
            repres = {names.bids: [], names.asks: []}

            decisionD = {names.asks: self.asks, names.bids: self.bids}
            for side in decisionD:
                for price in decisionD[side]:
                    amount = decisionD[side][price]
                    repres[side].append([str(price), str(amount)])

            return repres
        except Exception: raise

"""
class AutoVivification(SortedDict):
    #a = AutoVivification()
    #a[1][2][3] = 4 gives {1: {2: {3:4} } }
    #Implementation of perl's autovivification feature.
    def __getitem__(self, item):
        try: return SortedDict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

class VerifiedInput(object):
    #interface to SortedDict
    def __init__(self):
        # map: price -> oid->order
        self._orderbook = AutoVivification()
        # map: price -> amount->order for fast deletion/update
        self._amountMap = AutoVivification()    # not unique!
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __setitem__(self, _price, _Order):
        toPrint = (_price, _Order)
        assert type(_price) == int, toPrint
        assert type(_Order).__name__ == "Order", toPrint

        try:
            if _Order.amount > 0:
                # place into orderbook
                # .datePlaced should be unique
                assert _Order.datePlaced not in self._orderbook[_price], ("Order with same oid already a member!", self._orderbook[_price], _Order)
                # update all maps
                self._orderbook[_price][_Order.oid] = _Order
                self._amountMap[_price][_Order.amount][_Order.oid] = _Order
            # don't place into orderbook, just update orders at that price
            elif _Order.amount < 0:
                # since orders can be only added or cancelled, try to find oldest order with the same amount
                amountToLookFor = -_Order.amount
                if amountToLookFor in self._amountMap[_Order.price]:
                    oldestOID = self._amountMap[_Order.price][amountToLookFor].smallest_key()
                    oldestOrder = self._amountMap[_Order.price][amountToLookFor][oldestOID]
                    del self[oldestOrder]
                else:
                    #deleting procedure: start by removing amount from the oldest order
                    while _Order.amount < 0:    # the amount of incoming order is negative
                        oldestOID = self._orderbook[_Order.price].smallest_key()
                        oldestOrder = self._orderbook[_Order.price][oldestOID]
                        oldestOrderAmount = oldestOrder.amount
                        self.orderUpdate(oldestOrder, _Order.amount)    # _Order.amount is already negative
                        _Order.amount += oldestOrderAmount
        except Exception: raise
    def __getitem__(self, key): return self._orderbook[key]
    def __iter__(self, reverse=False):
        if reverse: return reversed(self._orderbook._sorted_keys)
        else: return iter(self._orderbook._sorted_keys)
    iterkeys = __iter__
    def orderUpdate(self, _Order, _updateAmount):
        try:
            toPrint = (_Order, _updateAmount)
            assert type(_Order).__name__ == "Order", toPrint
            assert type(_updateAmount) > 0, toPrint
            # should we delete the amount (we need order.amount to later update amount map) or just update it?
            originalAmount = _Order.amount
            originalAmount += _updateAmount
            # delete if amount == 0
            if originalAmount <= 0: del self[_Order]
            # update amount
            else:
                self._orderbook[_Order.price][_Order.oid].amount += _updateAmount
                #update amount map since some new amount is left:
                # remove old entry
                del self._amountMap[_Order.price][originalAmount][_Order.oid]
                # if no more orders at that amount, remove that amount
                if not self._amountMap[_Order.price][originalAmount]: del self._amountMap[_Order.price][originalAmount]
                # place new amount
                self._amountMap[_Order.price][_Order.amount][_Order.oid] = _Order
        except Exception: raise
    def __delitem__(self, _Order):
        # update all maps
        try:
            assert type(_Order).__name__ == "Order", _Order
            if _Order.head:
                # is one of the targets, so since this target is empty, remove reference from head
                _Order.head.targets[:] = [activeTarget for activeTarget in _Order.head.targets if activeTarget is not _Order]
            # update maps
            del self._orderbook[_Order.price][_Order.oid]
            del self._amountMap[_Order.price][_Order.amount][_Order.oid]
            # remove prices with no depth (without orders)
            if not self._orderbook[_Order.price]:
                del self._orderbook[_Order.price]
                del self._amountMap[_Order.price]
        except Exception: raise
    def getVolumeAtPrice(self, _price):
        try:
            assert type(_price) == int, _price
            volume_ = 0
            for order in self._orderbook[_price]:
                volume_ += order.amount
            return volume_
        except Exception: raise

    def orderRemove(self, _Order):
        try:
            assert type(_Order).__name__ == "Order", _Order
            self.orderUpdate(_Order, -_Order.amount)
        except Exception: raise

class ResearchOrderbook(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self):
        try:
            self.asks = VerifiedInput()
            self.bids = VerifiedInput()
        except Exception: raise
    def onDelta(self, _IPCDelta):
        try:
            orderbook = self.asks if _IPCDelta.type == IPC.normAsk else self.bids
            orderbook[_IPCDelta.price] = Order(_IPCDelta.price, _IPCDelta.delta, _IPCDelta.date,
                                               _market=_IPCDelta.market, _type=IPCDelta.typeNorm2Str(_IPCDelta.type))
        except Exception: raise


class __OrderbookManager(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self): self._orderbook = SortedDict()
    def update(self, _Order):
        try:
            assert _Order.__class__.__name__ == "Order", _Order
            price = _Order.price
            try:
                self._orderbook[price].amount += _Order.amount
                amount = self._orderbook[price].amount
                if amount == 0: del self._orderbook[price]
                elif amount < 0: raise AssertionError("Orderbook updated to negative amount", price)
            except KeyError: self._orderbook[price] = _Order
        except Exception: raise
    def __getitem__(self, key): return self._orderbook[key]
class _BidsManager(__OrderbookManager):
    def __init__(self): super(_BidsManager, self).__init__()
    def __iter__(self): return reversed(self._orderbook._sorted_keys)
    iterkeys = __iter__
class _AsksManager(__OrderbookManager):
    def __init__(self): super(_AsksManager, self).__init__()
    def __iter__(self): return iter(self._orderbook._sorted_keys)
    iterkeys = __iter__
class Orderbook(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self):
        self.bids = _BidsManager()
        self.asks = _AsksManager()


class __ActiveOrdersManager(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __getitem__(self, key): return self._orderbook[key]
    def __init__(self, *args):
        try:
            self._orderbook = AutoVivification()
            self._oidMap = {}
            self._observers = {}
            self.observerAdd(*args)
            assert "Account" in self._observers, ("Missing observers", self._observers)
        except Exception: raise
    def observerAdd(self, *args):
        try:
            for instance in args: self._observers[instance.__class__.__name__] = instance
        except Exception: raise

    def update(self, _Order):
        try:
            price = _Order.price
            oid = _Order.oid
            try: self._oidMap[oid].amount += _Order.amount
            except KeyError:
                self._orderbook[price][oid] = _Order
                self._oidMap[oid] = _Order
            # clean stuff
            if self._oidMap[oid].amount == 0:
                del self._oidMap[oid]
                del self._orderbook[price][oid]
                if not self._orderbook[price]:
                    del self._orderbook[price]
            elif self._oidMap[oid].amount < 0: raise AssertionError("Orderbook updated to negative amount", price)
            # trigger observers
            self._observers["Account"]
        except Exception: raise
    def place(self, _Order):
        try:
            assert _Order.oid not in self._orderbook[_Order.price], ("Same OID already exists at this price", _Order)
            self.update(_Order)
        except Exception: raise
    def cancel(self, _Order):
        try:
            _Order.amount = -_Order.amount
            self.update(_Order)
        except Exception: raise
"""