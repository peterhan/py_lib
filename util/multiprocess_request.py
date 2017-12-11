import requests
import json
import traceback 
import itertools
from multiprocessing import Pool

       
def requeset_wrapper(x):
    try:
        print x
        return requests.request(**x[0]),x[1]
    except Exception as e:        
        print 'Fail url:',x,e
        return False,x    
        
def mp_request(urls,keys,datas=None,method='get',processes=2):
    p = Pool(processes)
    if datas is not None:
        request_inputs=[{'method':method,'url':u,'data':d} for u,d in zip(urls,datas)]
    else:
        request_inputs=[{'method':method,'url':u} for u in urls]    
    args=zip(request_inputs,keys)
    res=p.map(requeset_wrapper,args)
    return res
 
    
    
if __name__=='__main__':
    
    