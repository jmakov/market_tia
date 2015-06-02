import logging
import collections
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.market.targetManager as tm; reload(tm)
import tia.trad.market.strat as strat; reload(strat)
import tia.trad.market.features as features; reload(features)
import tia.trad.market.account as acc; reload(acc)
import tia.trad.market.orderbook as orderbook; reload(orderbook)
import tia.trad.market.filters as filters; reload(filters)
from tia.trad.tools.sortedDict import SortedDict
import tia.trad.tools.ipc.processLogger as pl

LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
DB_ITEM_PRECISION = 8; DB_FUNDS_PRECISION = 5


def getCurrencies(_marketObjL):
    # get all currencies
    currencies = {}
    for market in _marketObjL:
        try:
            name = market.__class__.__name__
            currency = name[-3:]
            currencies[currency] = currency
        except Exception: raise
    return currencies.keys()


class Market:
    #def __repr__(self): return "%s(%s)" % (type(self).__name__, self.__dict__)
    logger = logging.getLogger(LOGGER_NAME + ".Market")
    exchangeRates = {"USD": fl.D("1"), "EUR": fl.D("1"), "GBP": fl.D("1.51"), "PLN": fl.D("0.32")}    # USD/EUR etc.
    targetManager = tm.TargetManager()
    reconnect_mode = 0
    minAllowableBet = fl.D("0.01")
    depthAddrS = None; depthAddrE = None
    tradesAddrS = None; tradesAddrE = None
    feeMakerDefault = None; feeTakerDefault = None

    def config(self):
        try:
            self.router = None
            # market properties
            self.lastUpdate = 0
            self.lastTid = 0
            # market state
            self.account = acc.Account()
            self.account.setFee(self.feeMakerDefault, self.feeTakerDefault)

            self.orderbook = orderbook.SortedOrderbook()
            self.activeOrders = orderbook.ActiveOrders(self.account)
            self.activeStrategies = strat.ActiveStrategies()
            self.ordersQueue = collections.deque()

            self.trades = SortedDict()

            self.currency = self.__class__.__name__[-3:]

            self.filters = filters.Filter()
            self.dataWindow = features.DataWindow()

            self.name = self.__class__.__name__
            # set addresses
            if "btc24" in self.name:
                self.depthAddr = self.depthAddrS + self.currency + self.depthAddrE
                self.tradesAddr = self.tradesAddrS + self.currency + self.tradesAddrE
            elif "btce" in self.name:
                self.depthAddr = self.depthAddrS + self.currency.lower() + self.depthAddrE
                self.tradesAddr = self.tradesAddrS + self.currency.lower() + self.tradesAddrE
            elif "bitstamp" in self.name:
                self.depthAddr = self.depthAddrS
                self.tradesAddr = self.tradesAddrS
            elif "mtgox" in self.name:
                self.depthAddr = self.depthAddrS
                self.tradesAddr = self.tradesAddrS
                self.lagAddr = "https://mtgox.com/api/1/generic/order/lag"
            elif "intrsng" in self.name:
                currencyId = None
                for id, name in self.idNameMap.items():
                    if self.name == name: currencyId = id
                self.tradesAddr = self.tradesAddrS + self.tradesAddrE + currencyId
                self.depthAddr = self.depthAddrS + self.depthAddrE + currencyId
            else:
                self.depthAddr = self.depthAddrS + self.currency + self.depthAddrE
                self.tradesAddr = self.tradesAddrS + self.currency + self.tradesAddrE

            # set smallest increments of price/amount
            if "btce" in self.name:
                self.pip = fl.D("0.001")
                self.pipAmount = fl.D("0.00000001")
            elif "bitstamp" in self.name:
                self.pip = fl.D("0.01")
                self.pipAmount = fl.D("0.00000001")
            elif self.name[:-3] in ["mtgox", "btc24"]:
                self.pip = fl.D("0.00001")
                self.pipAmount = fl.D("0.00000001")
            elif self.name[:-3] in "intrsng":
                self.pip = fl.D("0.00001")
                self.pipAmount = fl.D("0.00001")
            else: raise Exception("unhandled market")

            # set private API addresses
            if "btc24" in self.name:
                self.minAllowableBet = fl.D("0.09")
                oneApi = "https://bitcoin-24.com/api/user_api.php"
                self.apiBalanceAddr = oneApi
                self.apiTransactionsAddr = oneApi
                self.apiOpenOrdersAddr = oneApi
                self.apiCancelOrdersAddr = oneApi
                self.apiBuyOrderAddr = oneApi
                self.apiSellOrderAddr = oneApi
                self.apiBTCDepositAddr = oneApi
                self.apiBTCWithdrawalAddr = oneApi
        except Exception: raise
    def sendEvent(self, _Event):
        try:
            logger.debug("sendEvent: %s" % _Event)
            self.ordersQueue.append(_Event)
            self.filters.synchedWithExchange = 0
        except Exception: raise


class Btce(Market):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Btce")
    feeMakerDefault = fl.D("0.002"); feeTakerDefault = fl.D("0.002")    # 0.2 %
    depthAddrS = "https://btc-e.com/api/2/btc_"; depthAddrE = "/depth"
    tradesAddrS = "https://btc-e.com/api/2/btc_"; tradesAddrE = "/trades?since="
class btceUSD(Btce):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Btce.btceUSD")
    def __init__(self):
        try: self.config()
        except Exception: raise


class Btc24(Market):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Btce24")
    feeMakerDefault = 0; feeTakerDefault = 0  # https://bitcoin-24.com/more
    depthAddrS = "https://bitcoin-24.com/api/"; depthAddrE = "/orderbook.json"
    tradesAddrS = "https://bitcoin-24.com/api/"; tradesAddrE = "/trades.json?since="
class btc24EUR(Btc24):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Btce24.btce24EUR")
    def __init__(self):
        try: self.config()
        except Exception: raise
class btc24USD(Btc24):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Btce24.btce24USD")
    def __init__(self):
        try: self.config()
        except Exception: raise


class Bitstamp(Market):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Bitstamp")
    feeMakerDefault = fl.D("0.005"); feeTakerDefault = fl.D("0.005")
    depthAddrS = "https://www.bitstamp.net/api/order_book/"; depthAddrE = ""
    tradesAddrS = "https://www.bitstamp.net/api/transactions/"; tradesAddrE = ""
class bitstampUSD(Bitstamp):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Bitstamp.bitstampUSD")
    def __init__(self):
        try: self.config()
        except Exception: raise


class MtGox(Market):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.MtGox")
    feeMakerDefault = fl.D("0.006"); feeTakerDefault = fl.D("0.006")
    depthAddrS = "http://data.mtgox.com/api/2/BTCUSD/money/depth/fetch"; depthAddrE = ""
    tradesAddrS = "http://data.mtgox.com/api/2/BTCUSD/money/trades/fetch"; tradesAddrE = ""
    tickerAddr = "http://data.mtgox.com/api/2/BTCUSD/money/ticker"
class mtgoxUSD(MtGox):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.MtGox.mtgoxUSD")
    def __init__(self):
        try: self.config()
        except Exception: raise


class Intersango(Market):
    cllogger = logging.getLogger(pl.PROCESS_NAME + "markets.Intersango")
    feeMakerDefault = fl.D("0.0035"); feeTakerDefault = fl.D("0.0095")
    depthAddrS = "https://intersango.com/api/depth.php"; depthAddrE = "?currency_pair_id="
    tradesAddrS = "https://intersango.com/api/trades.php"; tradesAddrE = "?currency_pair_id="
    idNameMap = {"1":"intrsngGBP", "2": "intrsngEUR", "3": "intrsngUSD", "4": "intrsngPLN"}
class intrsngGBP(Intersango):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Intersango:intrsngGBP")
    def __init__(self):
        try: self.config()
        except Exception: raise
class intrsngEUR(Intersango):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Intersango:intrsngEUR")
    def __init__(self):
        try: self.config()
        except Exception: raise
class intrsngPLN(Intersango):
    logger = logging.getLogger(pl.PROCESS_NAME + "markets.Intersango:intrsngPLN")
    def __init__(self):
        try: self.config()
        except Exception: raise

