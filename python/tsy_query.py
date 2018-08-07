#!/usr/bin/python3

import pymysql
import json
import sys


def getdb(cfile) :
    cred = json.load(open(cfile))
    db = pymysql.connect(cred["s"], cred["u"], cred["p"], cred["d"])
    return db

def maxd(db):
    cursor = db.cursor()
    cursor.execute("select max(date) from yield_history")
    result = cursor.fetchone()
    return result[0]

def fields():
    return ["date", "3m", "6m", "1y", "2y", "3y", "5y", "7y", "10y", "30y"]

def latest(db):
    sel = ",".join(f for f in fields())
    qstr = "select {} from yield_history where date = {}".format(sel, maxd(db))
    cursor = db.cursor()
    cursor.execute(qstr)
    results = cursor.fetchall()
    for row in results:
        s = "  ".join(str(r) for r in row)
        break
    return s

try:
    db = getdb(sys.argv[1])
    print(latest(db))
    db.close()
except:
    print("some database error")

