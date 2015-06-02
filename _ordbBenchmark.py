from tia.trad.market.orderbook import RecorderOrderbook, Order
import time
NTIME = 10**5

def timer(func):
    def wrapper(*args):
        stime = time.time()
        func(*args)
        etime = time.time()
        print func.__name__, etime - stime
    return wrapper

@timer
def sdPlaceOrderDiffPrice(orderbook):
    for i in xrange(NTIME):
        orderbook.asks[i] = Order(i, 10**8, 10**8)
        #orderbook.bids[i] = Order(i, 10**8, 10**8)
@timer
def sdPlaceorderSamePrice(orderbook):
    for i in xrange(NTIME):
        orderbook.asks[1] = Order(1, 10**8, i)
@timer
def sdRemoveOrderSamePrice(orderbook):
    for i in xrange(NTIME):
        orderbook.asks[1] = Order(1, -10**8, i)
@timer
def sdRemoveOrderDiffPrice(orderbook):
    for i in xrange(NTIME):
        orderbook.asks[i] = Order(i, -10**8, i)


orderbook = RecorderOrderbook()
sdPlaceOrderDiffPrice(orderbook)
sdPlaceorderSamePrice(orderbook)
sdRemoveOrderSamePrice(orderbook)
sdRemoveOrderDiffPrice(orderbook)