"""

A variety of moving averages

"""

import numpy as np
import matplotlib.pyplot as plt

"""
Simple Moving Average
"""

def simpleMA(data, period) :
    assert len(data) > period
    result = np.zeros(len(data) - period)
    for i in range(0, len(data) - period) :
        result[i] = np.mean(data[i:i+period])
    return result

"""
Exponential moving average
"""
def ema(data, period) :
    assert len(data) > period
    a = 2.0/(period + 1.0)
    result = np.zeros(len(data))
    result[0] = data[0]
    for i in range(1, len(data)) :
        result[i] = a*data[i] + (1-a)*result[i-1]
    return result

"""
Double Exponential Moving Average
"""
def dema(data, period):
    assert len(data) > period
    emaX = ema(data, period)
    emadX = ema(data - emaX, period)
    return emaX + emadX

"""
Triple Expoential Moving Average
"""
def trix(data, period):
    return ema(ema(ema(data, period), period), period)

def main():
    period = 25
    N = 1000
    data = np.zeros(N)
    data[0] = 100.0
    for i in range(1,N):
        dx = 0.20*np.sin(np.pi/6.0 * i) + 0.80*(np.random.rand() - np.random.rand())
        data[i] = dx + data[i-1]

    ma = simpleMA(data, period)
    em = ema(data, period)
    de = dema(data, period)
    tx = trix(data, period)
    x = np.arange(len(data))
    plt.plot(x, data)
    plt.plot(x[:len(ma)], ma, 'r')
    plt.plot(x[:len(em)], em, 'b')
    plt.plot(x[:len(de)], de, 'y')
    plt.plot(x[:len(tx)], de, 'g')
    plt.show()
    print(np.mean(ma))

if __name__ == '__main__':
    main()

