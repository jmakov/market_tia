import time

def getTime():
    try:
        now = time.time()
        normTime = now * 10 ** 6
        return int(normTime)    # return time in nicroseconds
    except Exception: raise