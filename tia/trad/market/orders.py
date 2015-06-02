import logging
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class Order(object):
    logger = logging.getLogger(LOGGER_NAME + ".Order")
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self, market, price, amount,  side, datePlaced=None, targets=None, oid = None):
        try:
            assert type(price) in [str, unicode] or type(price) == fl.D, type(price)
            assert type(amount) in [str, unicode] or type(amount) == fl.D, type(amount)
            assert type(market) in [str, unicode], type(market)
            if targets:
                assert type(targets) == dict, type(targets)
            # set by namesDelta.update, Order.executed
            self.market = market
            self.datePlaced = int(datePlaced) if datePlaced else None
            self.oid = int(oid) if oid else None
            self.price = fl.D(price)
            self.amount = fl.D(amount)
            # execution on trade
            self.dateExecuted = None
            # set by execution/router
            self.dateArrivedOnExchange = None
            # set by strategy
            self.type = side        # names.asks|bids
            self.targets = targets   # order sees exits in member marketNames
        except Exception: raise
    def __setattr__(self, key, value):
        try:
            self.__dict__[key] = value
            if key in ["price", "amount"]:
                self.value = (self.price * self.amount).quantize(fl.QP)   # update worth in local currency
                if self.__dict__[key] < 0:
                    raise AssertionError("Negative amount in account!: %s", (self, key, value))
        except AttributeError: pass     # since self.amount doesn't exists when .price is set
        except Exception: raise

