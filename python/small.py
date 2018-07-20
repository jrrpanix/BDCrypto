import time
import numpy as np
import sys
import os
import json

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

def createCSV(input_files, output_file="./data.csv") :
    wf = open(output_file, "w")
    wf.write("price,quantity,timestamp,buy_or_sell,limit_or_market,\n")
    for f in input_files:
        try:
            json_data = json.load(open(os.path.join(f)))
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



def main():
    assert len(os.sys.argv) > 3
    top_dir = os.sys.argv[1]
    cutoff = int(sys.argv[2])
    out_dir = os.sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)
    assert os.path.exists(out_dir)
    for d in os.listdir(top_dir):
        p = os.path.join(top_dir, d)
        input_files = []
        for d1 in sorted(os.listdir(p)):
            dt = d1.split('_')[0]
            if len(dt) != 8 : continue
            if int(dt) > cutoff : break
            input_files.append(os.path.join(p, d1))
        output_file = os.path.join(out_dir, "{}.csv".format(d))
        print(input_files, output_file)
        createCSV(input_files, output_file)
        print()
    

if __name__ == '__main__':
    main()
