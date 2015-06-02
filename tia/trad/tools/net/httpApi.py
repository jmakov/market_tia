import xml.dom.minidom as minidom
import urllib
import urllib2
import json
import tia.trad.tools.sortedDict as sd
import tia.trad.tools.ipc.naming_conventions as names
from tia.trad.tools.timing import getTime
import logging
import tia.trad.tools.arithm.floatArithm as fl
import tia.trad.tools.ipc.processLogger as pl
LOGGER_NAME = pl.PROCESS_NAME + __file__.split("/")[-1]; logger = logging.getLogger(LOGGER_NAME)
MAX_RECONNECTIONS = 2


def getHtml(_url):
    try:
        logger.debug("getHtml: hi")

        html_ = ""
        website = None
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.13 (KHTML, like Gecko) Chrome/0.A.B.C Safari/525.13',
            'Referer': 'http://python.org'}
        request = urllib2.Request(_url, headers=headers)
        for attempt in range(MAX_RECONNECTIONS):
            try:
                # proxy = urllib2.ProxyHandler({'http': '127.0.0.1:8118'})
                # opener = urllib2.build_opener(proxy)
                # urllib2.install_opener(opener)
                logger.info("getHtml:urlopen: %s: attempt: %s" % (_url, attempt))
                website = urllib2.urlopen(request, timeout=10)
                break
            except Exception as ex:
                logger.exception("getHtml:urlopen:exception: %s" % ex)
                continue

        if website != None: html_ = website.read()  # downloads content

        logger.debug("html: %s" % html_)
        return html_
    except Exception as ex:
        logger.exception("getHtml:ex:%s, %s" % (_url, ex))
        return None


def ratesFromECB():
    try:
        logger.debug("ratesFromECB: hi")

        u1 = urllib2.urlopen('http://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml')
        dom = minidom.parse(u1)
        relativeToEUR = {}
        for elem in dom.getElementsByTagName('Cube'):
            if elem.hasAttribute('currency'):
                relativeToEUR[elem.attributes['currency'].value] = elem.attributes['rate'].value
        # add EUR
        relativeToEUR["EUR"] = "1"
        relative2USD = {}
        for currencyS in relativeToEUR:
            rate = relativeToEUR[currencyS]
            rel2USD = fl.D(relativeToEUR["USD"]) / fl.D(relativeToEUR[currencyS])
            relative2USD[currencyS] = str(rel2USD)

        return relative2USD
    except Exception as ex: logger.exception("ratesFromECB:ex: %s" % ex)
def getAndCacheExchangeRates():
    """
    input: _dict = {currency: 0}
    output: CACHED_EXCHANGE_RATES = {"USD": "1"...}
    """
    try:
        logger.debug("cacheExchangeRates: hi")

        global CACHED_EXCHANGE_RATES

        CACHED_EXCHANGE_RATES = ratesFromECB()
    except Exception as ex: logger.exception("getAndCacheExchangeRates:ex: %s" % ex)


"""
public API
"""
def getOrderbook(_Market):
    """
    return _orderbook = {"asks": [["price", "amount"], ...]}
    """
    try:
        logger.debug("getOrderbook: hi")

        marketName = _Market.name
        _Market.lastUpdate = getTime()

        orderbook_ = {names.asks: [], names.bids: []}

        # get orderbook
        html = getHtml(_Market.depthAddr)
        if html:
            parsedJson = json.loads(html, parse_float=str, parse_int=str)

            if "mtgox" in marketName:
                data = parsedJson["data"]
                for side in [names.asks, names.bids]:
                    for dct in data[side]:
                        orderbook_[side].append([dct["price"], dct["amount"]])
            else:
                # check that orderbook appropriate shape
                dummyAccess = parsedJson[names.asks][0][0]  # throws exception if not that shape
                orderbook_ = parsedJson
        else: orderbook_ = None
        return orderbook_
    except ValueError: return None  # if srv down or some maintainance msg
    except Exception as ex:
        logger.exception("getOrderbook:ex:%s: %s" % (marketName, ex))
        return None

def getTrades(_Market, _since=""):
    """
    :return: tradesSD_ = SD{tid:{"date":123, "price":123, "amount":123}}
    """
    try:
        logger.debug("getTrades: hi")

        marketName = _Market.name
        _Market.lastUpdate = getTime()

        tradesD_ = sd.SortedDict()
        # get data
        if "btc24" in _Market.name: html = getHtml(_Market.tradesAddr + str(_Market.lastTid))
        else: html = getHtml(_Market.tradesAddr + _since)
        if html:
            parsedJson = json.loads(html, parse_float=str, parse_int=str)
            if "mtgox" in marketName:
                data = parsedJson["data"]
                for dct in data:
                    if dct["primary"] == "Y":
                        tradesD_[int(dct["tid"])] = {"price": dct["price"], "amount": dct["amount"], "date": dct["date"]}
            else:
                for dct in parsedJson:
                    tradesD_[int(dct["tid"])] = {"price": dct["price"], "amount": dct["amount"], "date": dct["date"]}
        else: tradesD_ = None
        return tradesD_
    except ValueError: return None  # if srv down or some maintainance msg
    except Exception: raise


def getTicker(_Market):
    try:
        logger.debug("getLag: hi")

        marketName = _Market.name
        _Market.lastUpdate = getTime()

        eventMsg_ = None
        html = getHtml(_Market.tickerAddr)
        if html:
            parsedJson = json.loads(html, parse_float=str, parse_int=str)
            data = parsedJson["data"]
            if data["avg"]["currency"] == "USD":
                minAsk = data["sell"]["value"]
                maxBid = data["buy"]["value"]
                eventMsg_ = {"name": names.onTicker, "market": "mtgoxUSD", "minAsk": minAsk, "maxBid": maxBid, "date": data["now"]}

        return eventMsg_
    except ValueError: return None  # if srv down or some maintainance msg
    except Exception as ex:
        logger.exception("getTicker:ex:%s: %s" % (marketName, ex))
        return None

def getLag(_Market):
    try:
        logger.debug("getLag: hi")

        marketName = _Market.name
        _Market.lastUpdate = getTime()

        lag_ = None
        html = getHtml(_Market.lagAddr)
        if html:
            parsedJson = json.loads(html, parse_float=str, parse_int=str)
            data = parsedJson["return"]
            lag_ = data["lag"]

        return lag_
    except ValueError: return None  # if srv down or some maintainance msg
    except Exception as ex:
        logger.exception("getLag:ex:%s: %s" % (marketName, ex))
        return None


"""
private API
"""
def getAccountBalance(_Market):
    try:
        logger.debug("getAccountBalance: hi")

        res_ = {}
        if "btc24EUR" in _Market.name:
            values = {"user": _Market.account.user,
                      "key": _Market.account.apiKey,
                      "api": "get_balance"}
            data = urllib.urlencode(values)

            while 1:
                try:
                    req = urllib2.Request(_Market.apiBalanceAddr, data)
                    response = urllib2.urlopen(req)
                    jsonStr = response.read()
                    logger.debug("%s: %s" % (_Market.name, jsonStr))
                    break
                except urllib2.HTTPError as ex:
                    logger.exception("getAccountBalance:ex: %s" % ex)
                    continue
                except Exception: raise
            logger.debug("%s: %s" % (_Market.name, jsonStr))

            jsonD = json.loads(jsonStr, parse_float=str, parse_int=str)

            res_[names.fundsAvailable] = jsonD["eur"]
            res_[names.itemsAvailable] = jsonD["btc_available"]
            res_["market"] = _Market.name
            res_["name"] = names.onAccountBallance
        return res_
    except ValueError as ex:
        logger.exception("ValueError:ex: %s" % ex)
        return None
    except Exception: raise
def getOpenOrders(_Market):
    try:
        logger.debug("getOpenOrders: hi")

        res_ = {}
        if "btc24" in _Market.name:
            values = {"user": _Market.account.user,
                      "key": _Market.account.apiKey,
                      "api": "open_orders"}
            data = urllib.urlencode(values)
            while 1:
                try:
                    req = urllib2.Request(_Market.apiBalanceAddr, data)
                    response = urllib2.urlopen(req)
                    jsonStr = response.read()
                    logger.debug("%s: %s" % (_Market.name, jsonStr))
                    break
                except urllib2.HTTPError as ex:
                    logger.exception("getOpenOrders:ex: %s" % ex)
                    continue
                except Exception: raise
            logger.debug("%s: %s" % (_Market.name, jsonStr))

            jsonD = json.loads(jsonStr, parse_float=str, parse_int=str)

            parsedList = []
            for dct in jsonD:
                parsedData = {}
                parsedData["oid"] = int(dct["id"])
                parsedData["amount"] = dct["amount"]
                side = names.bids if int(dct["type"]) == 2 else names.asks  # opposite than in place order!
                parsedData["orderType"] = side
                parsedData["price"] = dct["price"]
                parsedData["eventDate"] = int(dct["date"]) * 10**6

                parsedList.append(parsedData)

            res_ = {"orders": parsedList}
            res_["market"] = _Market.name
            res_["name"] = names.onOpenOrders

        return res_
    except urllib2.HTTPError as ex:
        logger.error("HTTPError:ex: %s" % ex)
        return None
    except ValueError as ex:
        logger.exception("ValueError:ex: %s" % ex)
        return None
    except Exception:raise
def getTransactions(_Market):
    try:
        logger.debug("getTransactions: hi")

        res_ = {}
        if "btc24" in _Market.name:
            values = {"user": _Market.account.user,
                      "key": _Market.account.apiKey,
                      "api": "trades_json"}
            data = urllib.urlencode(values)

            req = urllib2.Request(_Market.apiTransactionsAddr, data)
            response = urllib2.urlopen(req)
            jsonStr = response.read()
            logger.debug("%s: %s" % (_Market.name, jsonStr))

            jsonL = json.loads(jsonStr, parse_int=str, parse_float=str)

            lastTransaction = jsonL[-1]
            lastTransactionTid = int(lastTransaction["tid"])
            lastTid = _Market.lastTid

            if lastTid == 0: _Market.lastTid = lastTransactionTid
            elif lastTransactionTid > lastTid:
                res_["transactions"] = {}
                #get all the executed prices of our orders from last tid
                for dct in reversed(jsonL):
                    if int(dct["tid"]) > lastTid:
                        res_["transactions"][dct["price"]] = dct["amount"]
                _Market.lastTid = lastTransactionTid

                res_["name"] = names.onTransactionsList
                res_["market"] = _Market.name


        return res_
    except ValueError as ex:
        logger.exception("ValueError:ex: %s" % ex)
        return None
    except Exception:raise
def cancelOrder(_Market, _oid, _side):
    try:
        logger.debug("cancelOrder: hi")

        res_ = {}

        if "btc24" in _Market.name:
            values = {"user": _Market.account.user,
                      "key": _Market.account.apiKey,
                      "api": "cancel_order",
                      "id": _oid}
            data = urllib.urlencode(values)

            while 1:
                try:
                    req = urllib2.Request(_Market.apiBalanceAddr, data)
                    response = urllib2.urlopen(req)
                    jsonStr = response.read()
                    logger.debug("%s: %s" % (_Market.name, jsonStr))
                    break
                except urllib2.HTTPError as ex:
                    logger.exception("cancelOrder:ex: %s" % ex)
                    continue
                except Exception: raise

            jsonD = json.loads(jsonStr, parse_float=str, parse_int=str)

            if int(jsonD["error"]) == 0:
                res_ = {"name": names.orderCancel, "market": _Market.name, "side": _side, "oid": int(_oid)}
            else: _Market.logger.critical("didn't cancel order: %s" % jsonD)

        return res_
    except KeyError:
        if "not exist" in jsonD["message"]:
            return names.noSuchOrder
        else: raise
    except Exception:raise
def placeOrder(_Market, _Order):
    try:
        logger.debug("placeOrder: hi")

        res_ = {}
        if "btc24EUR" in _Market.name:
            apiAction = "buy_btc" if _Order.type == names.bids else "sell_btc"
            values = {"user": _Market.account.user,
                      "key": _Market.account.apiKey,
                      "api": apiAction,
                      "amount": str(_Order.amount.quantize(_Market.pipAmount)),
                      "price": str(_Order.price.quantize(_Market.pip)),
                      "cur": "EUR"}
            data = urllib.urlencode(values)
            while 1:
                try:
                    req = urllib2.Request(_Market.apiBalanceAddr, data)
                    response = urllib2.urlopen(req)
                    jsonStr = response.read()
                    logger.debug("%s: %s" % (_Market.name, jsonStr))
                    break
                except urllib2.HTTPError as ex:
                    logger.exception("placeOrder:ex: %s" % ex)
                    continue
                except Exception: raise

            jsonD = json.loads(jsonStr, parse_float=str, parse_int=str)

            #side = names.bids if int(jsonD["type"]) == 1 else names.asks
            res_ = {"name": names.orderPlace, "market": _Market.name, "orderPrice": jsonD["price"], "orderAmount": jsonD["amount"],
                    "oid": int(jsonD["id"]), "orderType": _Order.type, "eventDate": int(jsonD["date"]) * 10**6, "foundTargets": _Order.targets}

        return res_
    except KeyError:
        if jsonD:
            if "enough" in jsonD["message"]: return names.accountNotEnoughFunds
        else: raise
    except Exception:raise
