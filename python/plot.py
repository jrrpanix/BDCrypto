import matplotlib.pyplot as plt
import sys
import numpy as np
import os

def main(fname, start, nplot) :
    print(fname)
    with open(fname) as f:
        x1, x2, t = [], [], [] 
        for i, line in enumerate(f):
            if i == 0 : continue
            s = line.split(",")
            if len(s) < 3 : continue
            if i > start and i  < start + nplot:
                x1.append(float(s[0]))
                x2.append(float(s[1]))
                t.append(float(s[2].strip()))
    L=10
    act = np.array(x1)[L:len(x1)]
    pred = np.array(x2)[0:len(x2)-L]
    t = np.array(t)[0:len(t)-L]
    a, = plt.plot(t, act, 'g', label='actual')
    p, = plt.plot(t, pred, 'r', label='predicted')
    plt.legend(handles=[a, p])
    plt.title(os.path.basename(fname).split(".")[0])
    plt.show()


if __name__ == '__main__':
    assert len(sys.argv) > 2
    fname, start = sys.argv[1], int(sys.argv[2])
    nplot = int(sys.argv[3]) if len(sys.argv) > 3 else 2500
    main(fname, start, nplot)
