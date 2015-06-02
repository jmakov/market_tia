import sys
import tables

import pytest

import tia.db_apis as apis
import tia.trad.market.m as m
from tia.trad.tools.io.db import DictDiffer

TEST_OUTPUT_DIR = "tia/tests/output/"
FUNDS = 10**5
ITEMS = 10**8

# env preparation
def prepareEnv():
	"""
	first cycle trades -> queue,
	second cycle trades -> table,
	third cycle -> trades read from table and compare sinceDate|sinceTid
	"""
	apis.db.DB_FILENAME = TEST_OUTPUT_DIR + "testDb.h5"
	apis.LOG_FILENAME = TEST_OUTPUT_DIR + "testLog.log"
	apis.RELOAD_RESOLUTION = 1  # relative func isn't executed since index diff is always 0
	apis.TRADES_RESOLUTION = 1  # executed on every cycle
	apis.CHECKPOINTS_RESOLUTION = 3
	apis.QUEUE_FLUSH_RESOLUTION = 2

# mocking stuff
#   handle scenarios: 2 same depths, 2 added, removed, changed
CURRENCIES_RATES = {"USD": "1", "EUR": "2", "GBP": "3"}

A4 = {}; B4 = {}
A3 = {6*FUNDS: ITEMS, 5*FUNDS: ITEMS - 2}; B3 = {4*FUNDS: ITEMS, 3*FUNDS: ITEMS - 2}
A2 = {5*FUNDS: ITEMS, 4*FUNDS: ITEMS - 1}; B2 = {3*FUNDS: ITEMS, 2*FUNDS: ITEMS - 1}
A1 = {3*FUNDS: ITEMS, 4*FUNDS: ITEMS}; B1 = {1*FUNDS: ITEMS, 2*FUNDS: ITEMS}
DUMMY_DEPTH = [ # asks:(removed: 6, 5)  bids:(removed 4, 3)
				{"asks": A4, "bids": B4},
				# asks:(added: 6, changed: 5, removed: 4)   bids:(added: 4, changed: 3, removed: 2)
				{"asks": A3, "bids": B3},
				# asks:(added: 5, changed: 4, removed: 3)   bids:(added: 3, changed: 2, removed: 1)
				{"asks": A2, "bids": B2},
				# 2x same depth
               {"asks": A1, "bids": B1},
               {"asks": A1, "bids": B1}
				]
# we raise ValueError in 5th cycle
T4 = {"date": 4, "price": FUNDS, "amount": ITEMS, "tid": 4}
T3 = {"date": 3, "price": FUNDS, "amount": ITEMS, "tid": 3}
T2 = {"date": 2, "price": FUNDS, "amount": ITEMS, "tid": 2}
T1 = {"date": 1, "price": FUNDS, "amount": ITEMS, "tid": 1}
def t4(sinceArg):
	if sinceArg < 10**9:
		assert sinceArg == 3
		return [{"date": 4, "price": FUNDS, "amount": ITEMS, "tid": sinceArg + 1}, {"date": 4, "price": FUNDS, "amount": ITEMS, "tid": sinceArg - 1}]
	else:
		return [T4, {"date": 4, "price": FUNDS, "amount": ITEMS, "tid": 2}]
def t3(sinceArg):
	# scenario: no trades in queue, trades in tables
	# allowed called args "from tables":
	if sinceArg < 10**9:
		assert sinceArg == 2
		return [{"date": 3, "price": FUNDS, "amount": ITEMS, "tid": sinceArg + 1}, {"date": 3, "price": FUNDS, "amount": ITEMS, "tid": sinceArg - 1}]
	else:
		return [T3, {"date": 3, "price": FUNDS, "amount": ITEMS, "tid": 1}]
def t2(sinceArg):
	# scenario: +1 trade in queue -> flush both to tables
	# allowed called args "from queue":
	if sinceArg < 10**9:
		assert sinceArg == 1
		return [{"date": 2, "price": FUNDS, "amount": ITEMS, "tid": sinceArg + 1}, {"date": 2, "price": FUNDS, "amount": ITEMS, "tid": sinceArg - 1}]
	else:
		return [T2, {"date": 2, "price": FUNDS, "amount": ITEMS, "tid": 0}]
def t1(sinceArg):
	# scenario: not in tables|queue -> get into queue
	# allowed called args "from else":
	if sinceArg == "": assert 1
	elif sinceArg == 86400: assert 1
	else: assert 0
	return [T1]
DUMMY_TRADES = [t4, t3, t2, t1]
DUMMY_TIME = [3, 3, 2, 2, 1, 1, 0]
def dummyParseDepth():
	global DUMMY_DEPTH;	return DUMMY_DEPTH.pop()
def dummyParseTrades(sinceArg):
	global DUMMY_TRADES; DUMMY_TRADES.pop()(sinceArg)
def dummyGoogleCalc(currencyToConvert):
	assert currencyToConvert in CURRENCIES_RATES
	return CURRENCIES_RATES[currencyToConvert]
def dummyGetTime():
	global DUMMY_TIME; return DUMMY_TIME.pop()

def validateCpORDeltas(_groupTable, _currency, _modeS):
	try:
		assert _modeS in ["checkpoints", "deltas"], "wrong mode: %s" % _modeS

		[tableA, tableB] = [getattr(_groupTable, "cpA"), getattr(_groupTable, "cpB")] if _modeS == "checkpoints" else [getattr(_groupTable, "asks"), getattr(_groupTable, "bids")]
		verificationA = [A1, A1, A2, A3, A4]; verificationB = [B1, B1, B2, B3, B4]

		decisionD = {verificationA: tableA, verificationB: tableB}
		for dataL in decisionD:
			rowIndex = 0; delIndex = 0
			for dataD in dataL:
				expectedD = {}
				# prepare expected data
				if _modeS == "checkpoints":
					for key in dataD: expectedD[key * CURRENCIES_RATES[_currency]] = dataD[key]     # localize verification data to market currency
				else:
					if dataD in [A4, B4]: pass  # we're done, no validation either since indexes out of bound
					else:
						d2 = dataL[delIndex + 1]; d1 = dataL[delIndex]
						diffD = DictDiffer(d2, d1)
						delIndex += 1

						added = diffD.added(); removed = diffD.removed(); changed = diffD.changed()

						if added:
							for key in added: expectedD[key * CURRENCIES_RATES[_currency]] = d2[key]
						if removed:
							for key in removed: expectedD[key * CURRENCIES_RATES[_currency]] = -d1[key]
						if changed:
							for key in changed: expectedD[key * CURRENCIES_RATES[_currency]] = d2[key] - d1[key]

						if delIndex == 1: assert expectedD == {}
				# validate
				if delIndex == 1: pass  # since no deltas and thus nothing is recorded to DB
				else:
					for row in decisionD[dataL].iterrows(rowIndex, rowIndex + 2):
						rowIndex += 1
						assert row["price"] in expectedD; assert row["amount"] == expectedD[row["price"]]
						del expectedD[row["price"]]     # so that we check the only left value is the one that is expected
						if not expectedD:   # when no data anymore to check, break
							break
	except Exception as ex:
		sys.stderr.write("\nvalidateCpORDeltas:ex: %s" % ex); sys.exit()
def validateTrades(_groupTable, _currency):
	tables = getattr(_groupTable, "trades")
	verificationT = [T1, T2, T3, T4]

	for index, row in enumerate(tables):
		assert row["date"] == verificationT[index]["date"] * 10**6
		assert row["tid"] == verificationT[index]["tid"]
		assert row["price"] == verificationT[index]["price"] * CURRENCIES_RATES[_currency]
		assert row["amount"] == verificationT[index]["amount"]

def checkDB(_DB):
	try:
		h5file = tables.openFile(_DB.filename, mode="r", filters=_DB.filters)
		groups = h5file.root._v_groups  # returns {'str(groupName)': getattr(_h5file.root, groupName)}
		for group in groups:
			currency = group[-3:]; assert currency in CURRENCIES_RATES
			groupTable = groups[group]; assert type(groupTable) == tables.table.Table
			validateCpORDeltas(groupTable, currency, "checkpoints")
			validateCpORDeltas(groupTable, currency, "deltas")
			validateTrades(groupTable, currency)
	except Exception as ex:
		sys.stderr.write("\ncheckDB:ex: %s" % ex); sys.exit()

def checkSuccEnd(_ex):
	assert _ex == "main: while loop:ValueError: no data in objects depth!"
def injectDummyMethods(_initD):
	m.callGoogleCalc = dummyGoogleCalc
	apis.getTime = dummyGetTime
	for market in _initD["markets"]:
		market.parseDepth = dummyParseDepth
		market.parseTrades = dummyParseTrades

def test_main():
	prepareEnv()

	with pytest.raises(ValueError) as ex:   # we end program with injecting empty depth to objects
		initD = apis.init()

		injectDummyMethods(initD)

		apis.runtime(initD["currenciesL"], initD["DB"], initD["markets"], initD["prevMarkets"])

		# test if program exits as expected
		checkSuccEnd(ex)

		# test prog generates expected results
		checkDB(initD["DB"])