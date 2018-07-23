from sklearn import preprocessing
from sklearn.decomposition import FastICA
from sklearn.decomposition import PCA
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
import numpy as np
import sklearn



def genX(N, m1, m2) :
    x1 = np.random.rand(N)*m1
    x2 = np.random.rand(N)*m2
    return np.column_stack((x1,x2))

def genY(X, m, sigma=1.0, mu=0.0):
    N = len(X)
    return X.dot(m) + (sigma*np.random.randn(N) + mu)

def runRegression(X, y):
    lr = LinearRegression(normalize=True)
    model = lr.fit(X,y)
    predict = model.predict(X)
    print("model score=", model.score(X,y))
    print(model.coef_)

def runRegressionPreprocess(X, y):
    ss = sklearn.preprocessing.StandardScaler()
    ss.fit(X)
    lr = LinearRegression()
    model = lr.fit(ss.transform(X), y)
    print(model.coef_)
    print(model.coef_/ss.scale_)

    
def main():
    coeff = np.array([2.0, 7.0])
    X = genX(100, 100, 500)
    y = genY(X, coeff)
    runRegression(X, y)
    runRegressionPreprocess(X, y)
    

if __name__ == "__main__" :
    main()

