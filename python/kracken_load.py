import krakenex
from requests.exceptions import HTTPError
import json
import time
import os

def getTime(t):
    tm = time.gmtime(t)
    #ts = time.strftime('%m/%d/%Y %H:%M:%S', tm)
    ts = time.strftime('%Y%m%d_%H%M%S', tm)
    return ts


#
# OHLC - interval data (O) open, (H) high, (L) low, (C) close
# 'since' : 0
# 'pair'  : 'XXBTZUSD'
# 'interval' : 1
def getOHLC(pair, interval, since) :
    kraken = krakenex.API()
    try:
        response = kraken.query_public('OHLC', {'pair': pair, 'interval' : interval, 'since' : since})
        print(response)
        #pprint.pprint(response)
    except HTTPError as e:
        print(str(e))

def getTrades(pair, since) :
    kraken = krakenex.API()
    try:
        response = kraken.query_public('Trades', {'pair': pair, 'since' : since})
        return response
    except HTTPError as e:
        print(str(e))
        return None

def writeTrades(t, trades):
    fname = "{}.json".format(t)
    with open(t, "w") as f :
        f.write(json.dumps(trades))

def getLast():
    if os.path.isfile("./last") is False:
        return 0
    data = open("./last")
    last = 0
    for line in data:
        test = int(line)
        if test > last:
            last = test
    return last

def testTrades():
    since = getLast()
    pair = 'XXBTZUSD'

    lfp = open("./last", "a")
    for i in range(1000):
        trades = getTrades(pair, since)
        if not  'result' in trades : continue
        res = trades['result']
        for k in res.keys():
            if k == 'last':
                since = res[k]

                lfp.write( "%s\n" % str(since))
            else :
                rk = res[k]
                firstTime = getTime(rk[0][2])
                writeTrades(firstTime, trades)
                t0 = "NA" if k == 'last' else getTime(rk[0][2])
                tN = "NA" if k == 'last' else getTime(rk[-1][2])
                print(t0, tN, len(rk))

# interval = 1
# getOHLCData(pair, interval, since)
#    with open("./tmp.json", "w") as f :
#        f.write(json.dumps(trades, indent=4))

def readJson():
    with open("./tmp.json") as f:
        j = json.load(f)
        assert 'result' in j
        res = j['result']
        for k in res.keys():
            rk = res[k]
            if k == 'last' :
                print(rk)
            else:
                t0 = "NA" if k == 'last' else getTime(rk[0][2])
                tN = "NA" if k == 'last' else getTime(rk[-1][2])

                print(k, len(rk), rk[0], t0, rk[-1], tN)
                for r in rk:
                    print(r, getTime(r[2]))


if __name__ == '__main__':
    #readJson()
    t = time.time()
    tm = time.gmtime(t)
    print(tm)
    #ts = time.strftime('%m/%d/%Y %H:%M:%S', tm)
    ts = time.strftime('%Y%m%d_%H%M%S', tm)
    print(ts)
    testTrades()
