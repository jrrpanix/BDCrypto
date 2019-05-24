import krakenex
from requests.exceptions import HTTPError
import json
import time
import os
import sys
import argparse

#
# return time from epoch in seconds to a readable string
#
def formatTime(t,fmt=0):
    tm = time.gmtime(t)
    if fmt == 0 : return time.strftime('%Y%m%d_%H%M%S', tm)
    return time.strftime('%m/%d/%Y %H:%M:%S', tm)
#
# express current time as nanoseconds from epoch
#
def getTimeNowNS() :
    return int(time.time()*1000000000)

#
# get nanoseconds from epoch, given year YYYY , month 1-12 and day 1-31
#
def getTimeEpochNSfromYMD(yyyymmdd):
    ys = str(yyyymmdd)
    year, month, day = int(ys[0:4]), int(ys[4:6]), int(ys[6:8])
    dstr = "{}/{}/{} 00:00:00".format(month, day, year)
    tm = time.strptime(dstr, '%m/%d/%Y %H:%M:%S')
    t = time.mktime(tm)
    return int(t*1000000000)


#
# get the individual asset names
#
def getAssets() :
    kraken = krakenex.API()
    try:
        response = kraken.query_public('Assets', {})
        return response
    except HTTPError as e:
        print(str(e))
    return None

#
# get the asset pairs that trade
#
def getAssetPairs() :
    kraken = krakenex.API()
    try:
        response = kraken.query_public('AssetPairs', {})
        return response
    except HTTPError as e:
        print(str(e))
    return None


#
# OHLC - interval data (O) open, (H) high, (L) low, (C) close
# 'since' : 0
# 'pair'  : 'XXBTZUSD'
# 'interval' : 1
def getOHLC(pair, interval, since) :
    kraken = krakenex.API()
    try:
        response = kraken.query_public('OHLC', {'pair': pair, 'interval' : interval, 'since' : since})
        return response
    except HTTPError as e:
        print(str(e))
    return None

def getTrades(pair, since) :
    kraken = krakenex.API()
    error_count = 0
    #retries until error threshold hit
    while True:
        try:
            response = kraken.query_public('Trades', {'pair': pair, 'since' : since})
            return response
        except Exception as e:
            error_count += 1
            time.sleep(pow(2, error_count)) # exponential backoff
            if error_count >= 5:
                print(str(e))
                return None

def writeTrades(t, trades):
    with open(t, "w") as f :
        f.write(json.dumps(trades))

def getLast(last_file="./last"):
    if os.path.isfile(last_file) is False:
        return 0
    data = open(last_file)
    last = 0
    for line in data:
        test = int(line)
        if test > last:
            last = test
    return last


# interval = 1
# getOHLCData(pair, interval, since)
#    with open("./tmp.json", "w") as f :
#        f.write(json.dumps(trades, indent=4))

def readJson(jsonData):
    if not 'result' in jsonData: return None
    res = jsonData['result']
    for k in res.keys():
        rk = res[k]
        if k == 'last' :
            continue
        else:
            print(k, len(rk), rk[0], rk[-1])

def createCSV(data_dir, output_file="./data.csv") :
    wf = open(output_file, "w")
    wf.write("price,quantity,timestamp,buy_or_sell,limit_or_market,\n")
    for f in sorted(os.listdir(data_dir)):
        try:
            json_data = json.load(open(os.path.join(data_dir, f)))
            if not 'result' in json_data: continue
            result_json = json_data['result']
            pair = None
            for key in result_json.keys() :
                if not key == 'last' :
                    pair = key
                    break
            assert pair is not None
            trade_data = result_json[pair]
            for r in trade_data:
                line = "{},{},{},{},{},{}\n".format(r[0], r[1], r[2], r[3],r[4],r[5])
                wf.write(line)
        except:
            print(f)
#
# get the list of currencies offered on kraken
#
def showAssets() :
    results = getAssets()
    assert results is not None
    for key, value in results['result'].items():
        altname = value['altname']
        dec = value['decimals']
        disp = value['display_decimals']
        fstr = "{},{},{},{}".format(key, altname, dec, disp)
        print(fstr)

def showAssetPairs():
    results = getAssetPairs()
    assert results is not None
    for key, value in results['result'].items():
        if 'USD' in value['quote'] :
            fstr = "{},{},{}".format(key, value['base'], value['quote'])
            print(fstr)

def showMarketSnap():
    results = getAssetPairs()
    assert results is not None
    for key, value in results['result'].items():
        if not 'USD' in value['quote'] : continue
        if ".d" in key : continue
        ohlc_results = getOHLC(key, 1, 0)
        ohlc = ohlc_results['result'][key]
        start, end = ohlc[0][0], ohlc[-1][0]
        sclose, eclose = float(ohlc[0][4]), float(ohlc[-1][4])
        sstr, estr = formatTime(start, 1), formatTime(end, 1)
        pct = (eclose - sclose)/eclose * 100.0
        print( "%10s start: %s %12.6f, end: %s %12.6f : var %10.6f" % (key, sstr, sclose, estr, eclose, pct))


def createDataSet(dataDir, pair, sinceDate=0, toDate=0) :
    results = getAssetPairs()
    assert results is not None
    assert pair in results['result'].keys()
    toDate = getTimeEpochNSfromYMD(toDate) if toDate > 0 else getTimeNowNS()
    assert toDate > sinceDate
    if not os.path.exists(dataDir) :
        os.makedirs(dataDir)
        print("created", dataDir)
    lastFile = os.path.join(dataDir, "last")
    lastUpdate = getLast(lastFile)
    print(lastUpdate, toDate)
    sinceDate = sinceDate if sinceDate == 0 else getTimeEpochNSfromYMD(sinceDate)
    sinceDate = max(sinceDate, lastUpdate)
    lfp = open(lastFile, "a")
    faultCount = 0
    while sinceDate < toDate :
        trades = getTrades(pair, sinceDate)
        time.sleep(1) # throttle requests to avoid exchanging timing out
        if not 'result' in trades:
            faultCount += 1
            if faultCount > 6:
                # too many failures wait
                print("pausing exchange requests, sleeping for 10 seconds")
                time.sleep(10) # too many failures
            assert faultCount < 12
            continue
        res = trades['result']
        faultCount = 0
        for k in res.keys():
            if k == 'last':
                lfp.write( "%s\n" % str(sinceDate))
                sinceDate = int(res[k])
            else :
                rk = res[k]
                firstTime = formatTime(rk[0][2])
                outputFile = os.path.join(dataDir, "{}.json".format(firstTime))
                writeTrades(outputFile, trades)
                t0, tN = formatTime(rk[0][2]), formatTime(rk[-1][2])
                print("processing trades from %s to %s count = %d" % (t0, tN, len(rk)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="kraken api options")
    parser.add_argument('--run', choices=['csv', 'load', 'show'])
    parser.add_argument('--output_dir')
    parser.add_argument('--pair')
    parser.add_argument('--csv_file')
    parser.add_argument('--from_date', default=0, type=int)
    parser.add_argument('--to_date', default=0, type=int)
    args = parser.parse_args()
    assert args.run is not None

    if args.output_dir is None: args.output_dir = args.pair
    if args.csv_file is None and args.pair is not None: args.csv_file = args.pair + ".csv"
    if args.run == 'load': assert args.output_dir is not None and args.pair is not None
    if args.run == 'csv' : assert args.csv_file is not None and args.output_dir is not None

    if args.run == 'load' :
        createDataSet(args.output_dir, args.pair, args.from_date, args.to_date)
    if args.run == 'csv' :
        createCSV(args.output_dir, args.csv_file)
    if args.run == 'show':
        showAssetPairs()
