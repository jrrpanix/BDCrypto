import time
import datetime
import numpy as np
import sys
import os
import json

#
# return time from epoch in seconds to a readable string
#
def formatTime(t):
    if type(t) is datetime.datetime:
        return t.strftime('%Y%m%d_%H%M%S')
    else:
        tm = time.gmtime(t)
        return time.strftime('%Y%m%d_%H%M%S', tm)
    #return time.strftime('%m/%d/%Y %H:%M:%S', tm)

def timeBucket(tfloat):
    t = int(tfloat)
    tm = time.gmtime(t)
    return time.strftime('%Y%m%d%H%M', tm)
    
    
def timeIsMissing(last_time, current_time, gap=60) :
    if current_time < last_time + gap : return False
    eBucket = int(timeBucket(last_time + gap))
    nextBucketInData = int(timeBucket(current_time))
    missing = nextBucketInData > eBucket
    return missing
    
#return current_time > last_time + gap
    
def getMinutes(t):
    tm = time.gmtime(t)
    m = time.strftime('%M', tm)
    return int(m)
    
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

def linestr(price, qty, timeasfloat, s, m) :
    return ('%f,%f,%f,%s,%s,%s') % (price, qty, timeasfloat, s, m, timeBucket(int(timeasfloat)))
    
def bucketData(input_file, output_file):
    outf = open(output_file, "w")
    with open(input_file) as f:
        for i, line in enumerate(f):
            if i == 0 :
                continue
            v = line.split(",")
            if len(v) < 5 :
                continue
            price, qty, current_time, s, m = float(v[0]), float(v[1]), float(v[2]), v[3], v[4]
            time_in_seconds = int(current_time)
            if i == 1:
                last_px, last_time = price, current_time
            
            while timeIsMissing(last_time, current_time) :
                last_time += 60
                outf.write( "%s\n" % linestr(price, 0, last_time, s, m))
            outf.write( "%s\n" % linestr(price, qty, current_time, s, m))
            last_px , last_time = price, current_time

def main():
    assert len(os.sys.argv) > 2
    in_dir = os.sys.argv[1]
    out_dir = os.sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)
    for p in os.listdir(in_dir):
        input_file = os.path.join(in_dir,p)
        output_file = os.path.join(out_dir, p)
        bucketData(input_file, output_file)

if __name__ == '__main__':
    main()
