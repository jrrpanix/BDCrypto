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


print(formatTime(1381095255.5514))
print(formatTime(1495122416.9359))
print(formatTime(1382621686.0676))
print(formatTime(1438956205.7754))
print(formatTime(1527825618))

print(formatTime(1530704873.4625))

