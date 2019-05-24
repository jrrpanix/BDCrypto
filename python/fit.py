import numpy as np
import matplotlib.pyplot as plt

def parse(fname):
    pv, qv, tv = [], [], []
    with open(fname) as f :
        for i, line in enumerate(f):
            if i == 0 : continue
            sv = line.split(",")
            if len(sv) < 3 : continue;
            p, q, t = float(sv[0]), float(sv[1]), float(sv[2])
            pv.append(p)
            qv.append(q)
            tv.append(t)
    return pv, qv, tv

def plotData(pv, qv, tv):
    dt = np.zeros(len(tv))
    for i in range(1,len(tv)):
        dt[i] = tv[i] - tv[0]
    y = np.array(pv)
    plt.plot(dt, y)
    plt.show()

class ema :
    
    def __init__(self, period) :
        self.p = period
        self.a = 2.0/(period + 1.0)
        self.v = None
        
    def update(self, value) :
        if self.v is None : 
            self.v = value
        else:
            self.v = self.a * value + (1.0 - self.a) * self.v


class emaCalc :

    def __init__(self, period) :
        self.period = period
        self.ema = ema(period)
    
    def update(self, value) :
        self.ema.update(value)

    def get(self) :
        return self.ema.v


class dataSource :
    
    def __init__(self, name, fileName, period=10) :
        self.name = name
        self.p, self.q, self.t = parse(fileName)
        self.ix = 0
        self.emac = emaCalc(period)
        self.last = 0
        self.lastSamplePrice = 0

    def rewind(self):
        self.ix = 0

    def incr(self) :
        self.ix += 1

    def decr(self) :
        self.ix -= 1

    def hasNext(self) :
        return self.ix < len(self.t)

    def nextTime(self):
        if not self.ix < len(self.t) :
            print(self.name, self.ix, len(self.t))
        assert self.ix < len(self.t)
        return self.t[self.ix]

    def publish(self):
        i = self.ix
        sx = ("%s,%f,%f,%f" % (self.name, self.t[i], self.p[i], self.q[i]))
        return sx
    
    def update(self) :
        assert self.ix < len(self.p)
        self.last = self.p[self.ix]
        self.emac.update(self.p[self.ix])

    def get(self) :
        return self.emac.get()

    def num(self) :
        return len(self.p)
              
class indicatorUpdate :

    def __init__(self, name):
        self.name = name
        self.ma = []

def sourcesHasNext(sources):
    for s in sources:
        if s.hasNext() : return True
    return False

def nextSource(sources):
    next = -1
    for i, s in enumerate(sources) :
        if s.hasNext() :
            if next == -1 : next = i
            elif s.nextTime() < sources[next].nextTime() :
                next = i
    return next

def getLen(sources): 
    mx = 0
    for s in sources : mx += s.num()
    return mx


def marketSim(sources) :
    sampleFreq, count, longCount, numSamples = 20, 0, 0, 0
    dataLen = int(getLen(sources))
    numObs = int(dataLen / sampleFreq)
    matrix = np.zeros((numObs, len(sources)))
    response = np.zeros((numObs, len(sources)))
    print(matrix.shape)
    while sourcesHasNext(sources):
        next = nextSource(sources)
        sources[next].update()
        sources[next].incr()
        count += 1
        longCount += 1
        if count == sampleFreq :
            for i, s in enumerate(sources) :
                dx = s.last - s.get() if s.get() is not None and s.last is not None else 0
                matrix[numSamples, i] = dx
                dy = s.last - s.lastSamplePrice if s.lastSamplePrice != 0 else 0
                if numSamples > 0 : response[numSamples - 1, i] = dy
                s.lastSamplePrice = s.last
                    
            numSamples += 1
            count = 0
    
    xx_inv = np.linalg.inv(np.dot(matrix.transpose(), matrix))
    xy = np.dot(matrix.transpose(), response)
    beta = np.dot(xx_inv, xy)
    print(beta)

    print(matrix.max(axis = 0))
    print(response.max(axis = 0))
    print(numSamples)

def main():
    m1 = np.zeros(10*10).reshape((10,10))
    print(m1.shape)
    period = 500
    xrp = dataSource("xrp", "../data/xrp.csv", period)
    ltc = dataSource("ltc", "../data/ltc.csv", period)
    xbt = dataSource("xbt", "../data/xbt.csv", period)
    eth = dataSource("eth", "../data/eth.csv", period)
    marketSim([xrp, ltc, xbt, eth])
#    plotData(eth.p, eth.q, eth.t)
    

if __name__ == '__main__': 
    main()
