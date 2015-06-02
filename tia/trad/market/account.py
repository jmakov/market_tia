import tia.trad.tools.ipc.naming_conventions as names
import tia.trad.tools.arithm.floatArithm as fl
import logging
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)


class Account(object):
    def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    def __init__(self):
        try:
            self.reservedItems = fl.D("0")
            self.availableItems = fl.D("0")

            self.reservedFunds = fl.D("0")
            self.availableFunds = fl.D("0")

            self.userID = None
            self.username = None
            self.passw = None
            self.apiKey = None
            self.feeMaker = None
            self.feeTaker = None
        except Exception: raise
    def __setattr__(self, key, value):
        try:
            logger.debug("account:setattr: %s:%s" % (key, value))
            self.__dict__[key] = value

            if key in ["reservedItems", "availableItems", "reservedFunds", "availableFunds"]:
                if object.__getattribute__(self, key) < 0:
                    raise AssertionError("Negative amount in account!: %s", (self, key, value))
        except Exception: raise

    def setBalance(self, _reservedItems, _availableItems, _reservedFunds, _availableFunds):
        try:
            self.reservedItems = fl.D(_reservedItems)
            self.availableItems = fl.D(_availableItems)
            self.reservedFunds = fl.D(_reservedFunds)
            self.availableFunds = fl.D(_availableFunds)
        except Exception: raise
    def setUserCredentials(self, _exchangeName, _credentialsD):
        try:
            credoD = _credentialsD[_exchangeName]
            for attrib in credoD:
                credential = credoD[attrib]
                self.__dict__[attrib] = credential
        except KeyError:
            logger.info("no credentials for: %s" % _exchangeName)
        except Exception: raise
    def setFee(self, _feeMaker, _feeTaker):
        try:
            self.feeMaker = _feeMaker
            self.feeTaker = _feeTaker
        except Exception: raise
    def getResources(self, _side, _atPrice, _ChangedMarket):
        try:
            res = None
            if _side == names.bids:
                if _atPrice:
                    pip = _ChangedMarket.pip
                    res = ((self.reservedFunds + self.availableFunds) / _atPrice).quantize(pip, rounding=fl.cd.ROUND_DOWN)
                else: res = 0
            elif _side == names.asks:
                pipAmount = _ChangedMarket.pipAmount
                res = (self.reservedItems + self.availableItems).quantize(pipAmount, rounding=fl.cd.ROUND_DOWN)
            return res
        except Exception: raise