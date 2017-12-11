#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys

class MRTask:
    def __init__(self):
        pass
    def proc(self):
        pass
    def end(self):
        pass
    @staticmethod 
    def startup():
        defaultType='map1'
        type = defaultType
        if len(sys.argv)>1:
            type = sys.argv[1]
        cls = eval( type.capitalize() )
        obj=cls()
        obj.proc()
        obj.end()
    
def err_rpt( *info ):
    '''
    #'reporter:counter:<group>,<counter>,<amount>'
    input is counter,amount,group 
    '''
    counter = info[0]
    amount = info[1] if len(info)>=2 else 1
    group = info[2] if len(info)>=3 else 'custom_counter'
    st = 'reporter:counter:%s,%s,%s\n'%(group ,counter,amount)
    sys.stderr.write(st)

#w|!cat mid.txt|python mr.py map1 
if __name__=='__main__':
    MRTask.startup()
    
