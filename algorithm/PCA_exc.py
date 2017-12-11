import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def meanX(dataX):
    return np.mean(dataX,axis=0)
    
def pca(XMat,k):
    average=meanX(XMat)
    m,n=np.shape(XMat)
    data_adjust=[]
    avgs=np.tile(average,(m,1))
    data_adjust=XMat-avgs
    covX=np.cov(data_adjust.T)
    featValue,featVec=np.linalg.eig(covX)
    index=np.argsort(-featValue)
    finalData=[]
    if k>n:
        print 'k must lower than feature number'
        return
    else:
        selectVec=np.matrix(featVec.T[index[:k]])
        finalData=data_adjust*selectVec.T
        reconData=(finalData*selectVec)+average
    return finalData,reconData
    
def plotBestFit(data1, data2):    
    dataArr1 = np.array(data1)
    dataArr2 = np.array(data2)

    m = np.shape(dataArr1)[0]
    axis_x1 = []
    axis_y1 = []
    axis_x2 = []
    axis_y2 = []
    for i in range(m):
        axis_x1.append(dataArr1[i,0])
        axis_y1.append(dataArr1[i,1])
        axis_x2.append(dataArr2[i,0]) 
        axis_y2.append(dataArr2[i,1])                 
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(axis_x1, axis_y1, s=50, c='red', marker='s')
    ax.scatter(axis_x2, axis_y2, s=50, c='blue')
    plt.xlabel('x1'); plt.ylabel('x2');
    plt.savefig("outfile.png")
    plt.show()
    
def loaddata(datafile):
    return np.array(pd.read_csv(datafile,sep="\t",header=-1)).astype(np.float)    

def main():    
    datafile = "pca_data.txt"
    XMat = loaddata(datafile)
    k = 2
    return pca(XMat, k)
    
if __name__ == "__main__":
    finalData, reconMat = main()
    plotBestFit(finalData, reconMat)