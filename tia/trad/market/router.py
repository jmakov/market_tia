import collections


def synchronizeWithExchanges(_MarketsD):
    try:
        for Market in _MarketsD.values():
            if Market.router:
                # only if sth is there to send
                if len(Market.ordersQueue):
                    while 1:
                        try:
                            Event = Market.ordersQueue.popleft()
                            Market.router.put(Event)
                        except IndexError: break
    except Exception: raise