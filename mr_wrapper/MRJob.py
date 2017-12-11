#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys

class Opt:
    '''
    string, class, list, dict, int,boolean
    '''
    def __init__(self):
        self.type = 's'
        self.content = {
            's':None,'c' :None,
            'l' : None, 'd':None,'i':None ,'b':None}
        
class MRJob:
    def __init__(self):
        self.hdp_cmd = '/home/work/hadoop-client-stoff/hadoop/bin/hadoop'
        self.hdp_type = 'streaming' # streamoverhce
        self.input = None
        self.output = None
        self.mapper = ""
        self.reducer = ""
        self.paramDef = [
        'inputformat:c','outputformat:c',
        'partitioner:c','combiner:c',
        'cmdenv:d','inputreader:c','verbose:b','lazyOutput:b',
        'numReduceTasks:i','mapdebug:s','reducedebug:s',
        'conf:s','fs:s','archives:l','files:l']
        self.Dp = {}
        self.datestr = None
        
    def updateDp(self,newopt):
        self.Dp.update(newopt)
        
    def getDpStr(self):
        lst=[]
        for k,v in self.Dp.items():
            v=v.strip()
            if k.find('=')>0:
                lst.append( '-D '%( k ) )
            elif v.find(' ')>0 or v.find('\t')>0:
                lst.append( '-D %s="%s"'%(k,v) )
            else:
                lst.append( '-D %s=%s'%(k,v) )
        return ' '.join(lst)
        
    def getCmdStr(self):
        self.verifyProperty()
        cmd ='  '.join([ 
          self.hdp_cmd ,
          self.hdp_type,
          self.getDpStr(),
          self.getParams()])
        return cmd
        
    def getParams(self):
        return ""
          
    def prettyPrint(self,cmdStr):
        cmdStr=self.cmdStr.replace(" -", "\n\t -").replace("\n\t -p ", " -p ")        
        print cmdStr
        
    def __str__(self):
        pass
        
    def verifyProperty(self):
        assert self.input != None, "Input path is empty."
        assert self.output != None, "Output path is empty."
        assert self.mapper != None, "Mapper is empty."
        assert self.reducer != None, "Reducer is empty."
        
if __name__ == '__main__':
    mrj=MRJob()
    mrj.updateDp({'mapreduce.':' a bc'})
    mrj.input='/test/test/input'
    mrj.output='/test/test/output'
    print mrj.getCmdStr()