#!/home/work/python2.7/bin/python2.7
# -*- coding:utf-8 -*-

import os,getopt,sys,re
import subprocess as sp

class Job:
    def __init__(self):
        self.Hadoop="/home/work/hadoop-client-stoff/hadoop/bin/hadoop"
        self.mapper=None
        self.reducer=None
        self.input=None
        self.output=None
        self.mainFile=None
        self.projectName=None
        self.debugMapCmd=None
        self.debugInputFormat=None
        self.mapCapacity=500
        self.reduceCapacity=200
        self.reduceTask=10
        #chengong
        self.hasMapArgs=False
        self.hasReduceArgs=False
        # end chengong
        #start wangxuanchun
        self.argv=None
        #end wangxuanchun
        self.debugLength=100
        #self.pripority="HIGH"
        self.pripority="VERY_HIGH"
        self.smsAlarm=None # 手机短信报警
        self.emailAlarm=None # 邮件报警
        self.otherD=None # other -D options
        self.others=None # other options e.g. -cacheArchive /ps/ubs/bin/worddict.tar.gz#worddict
        self.keyFields=1
        self.partitionFields=1
        self.fileL=["*.py"]
        self.partitioner="Keybase" # or "Int"
        self.useHCE=False
        self.debugTmpFile="/tmp/hadoop_debug_dataxxx"
        ## compress option
        self.comp_map_flag="false" ## hadoop's boolean type
        self.comp_map_codec="org.apache.hadoop.io.compress.LzoCodec"
        self.comp_flag="false" ## hadoop's boolean type
        # self.comp_codec="org.apache.hadoop.io.compress.GzipCodec"
        self.comp_codec="org.apache.hadoop.io.compress.LzoCodec"

    def sanityCheck(self):
        assert self.input != None, "Input path is empty."
        assert self.output != None, "Output path is empty."
        assert self.mainFile != None, "Main program file is empty."
        assert self.projectName != None, "Project name is empty."

    def getExeCommand(self):
        if strDate=="":
            print "strDate not assigned"
            exit(0)
        if self.useHCE:
            cmdStr=self.Hadoop + " streamoverhce "
        else:
            cmdStr=self.Hadoop + " streaming "

        #cmdStr=self.Hadoop + " streaming "
        cmdStr+=" -D stream.num.map.output.key.fields="+"%d" % self.keyFields + \
                " -D mapred.job.priority=%s" % self.pripority + \
                " -D mapred.job.name=%s" % self.projectName + \
                " -D num.key.fields.for.partition=%d" % self.partitionFields + \
                " -D mapred.job.map.capacity=%d" % self.mapCapacity + \
                " -D mapred.job.reduce.capacity=%d" % self.reduceCapacity + \
                " -D mapred.reduce.tasks=%d" % self.reduceTask + \
                " -D mapreduce.map.output.compress=%s" % self.comp_map + \
                " -D mapreduce.map.output.compress.codec=%s" % self.comp_map_codec + \
                " -D mapred.output.compress=%s" % self.comp_flag + \
                " -D mapred.output.compression.codec=%s" % self.comp_codec + \
                ""

        if self.otherD!=None:
            for opt in self.otherD:
                cmdStr += " -D " + opt

        if self.partitioner=="Int":
            cmdStr += " -partitioner com.baidu.sos.mapred.lib.IntHashPartitioner "
        else:
            cmdStr += " -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner "

        if isinstance(self.input,list):
            for s in self.input:
                cmdStr+="  -input " + s
        else:
            cmdStr+="  -input " + self.input

        cmdStr+=" -output " + self.output

        cmdStr+=" -cacheArchive /ps/ubs/bin/python272.tar.gz#python "

        if self.others:
            if isinstance(self.others,list):
                for opt in self.others:
                    cmdStr+=" " + opt
            else: # 是string
                cmdStr+= " " + self.others

        for f in self.fileL:
            cmdStr += " -file "+f

        if callable(self.mapper): # is a function
            cmdStr += " -mapper \"python/python2.7/bin/python27.sh " + self.mainFile
            cmdStr+=" -p map\""
        else:
            cmdStr += " -mapper \"" + self.mapper + "\""

        if callable(self.reducer): # is a function
            cmdStr += " -reducer \"python/python2.7/bin/python27.sh " + self.mainFile
            cmdStr+=" -p reduce\""
        else:
            cmdStr += " -reducer \"" + self.reducer + "\" "

        return cmdStr

    def getRmCommand(self):
        cmdStr=self.Hadoop + " fs -rmr " + self.output
        return cmdStr

    def debugMap(self, isRun=True):
        if self.debugInputFormat=="zip":
            unzipCmd="zcat"
        elif self.debugInputFormat=="lzma":
            unzipCmd="lzcat"
        else:
            unzipCmd="cat"
        os.system("rm -rf "+self.debugTmpFile)
        # preparing data source
        for p in self.input:
            cmdStr=self.Hadoop + " fs -cat " + p + "/* 2>/dev/null | " + unzipCmd + " 2>/dev/null "\
                    " | head -n " + "%d"%self.debugLength + " >>" + self.debugTmpFile + " 2>/dev/null "
            #print cmdStr
            sys.stdout.flush()
            os.system(cmdStr)

        if callable(self.mapper): # is a function
                self.debugMapCmd="cat "+ self.debugTmpFile + " | python " + self.mainFile + " -p map "
        else:
                self.debugMapCmd="cat "+ self.debugTmpFile + " | " + self.mapper
        if isRun:
            #print self.debugMapCmd
            sys.stdout.flush()
            os.system(self.debugMapCmd)

    def debugReduce(self):
        if self.debugMapCmd==None:
            self.debugMap(False)
        if callable(self.reducer): # is a function
            cmd=self.debugMapCmd + " | sort | python " + self.mainFile + " -p reduce"
        else:
            cmd=self.debugMapCmd + " | sort | " + self.reducer
        #print cmd
        sys.stdout.flush()
        os.system(cmd)

    def show(self):
        cmd=self.Hadoop + " fs -cat " + self.output + "/*"
        os.system(cmd)

    def showInput(self):
        if self.debugInputFormat=="zip":
            unzipCmd="zcat"
        elif self.debugInputFormat=="lzma":
            unzipCmd="lzcat"
        else:
            unzipCmd="cat"
        # preparing data source
        for p in self.input:
            cmdStr=self.Hadoop + " fs -cat " + p + "/* 2>/dev/null | " + unzipCmd + " 2>/dev/null "\
                    " | head -n " + "%d"%self.debugLength + " >" + self.debugTmpFile + " 2>/dev/null "
            print cmdStr
            sys.stdout.flush()
            os.system(cmdStr)

        cmd="cat "+ self.debugTmpFile
        os.system(cmd)

    def ls(self):
        cmd=self.Hadoop + " fs -ls " + self.output
        print cmd
        sys.stdout.flush()
        os.system(cmd)

    def __printbetter(self,cmdStr):
        cmdStr=cmdStr.replace(" -", "\n\t -")
        cmdStr=cmdStr.replace("\n\t -p ", " -p ")
        print cmdStr

    def alarm(self):
        if self.smsAlarm != None:
            alarmStr="Hadoop 任务失败。任务名称: " + self.projectName
            smsCmd="gsmsend *3*"+self.smsAlarm+"\@\""+alarmStr+"。\""
            print smsCmd
            os.system(smsCmd)
        if self.emailAlarm != None:
            alarmStr="\"Hadoop 任务失败。任务名称：" + self.projectName + "\""
            emailCmd="LC_ALL=zh_CN mutt -a " + alarmStr + " " + self.emailAlarm
            print emailCmd
            os.system(emailCmd)

    def run(self):
        self.sanityCheck()
        rmCmd=self.getRmCommand()
        print rmCmd
        sys.stdout.flush()
        os.system(rmCmd)
        exeCmd=self.getExeCommand()
        self.__printbetter(exeCmd)
        sys.stdout.flush()
        os.system(exeCmd)
        # try to count the output folder. If task failed, will return 255
        lsCmd=self.Hadoop + " fs -count " + self.output
        p=sp.Popen(lsCmd, shell = True, stdout=sp.PIPE, stderr=None)
        output=p.communicate()[0]
        if p.returncode==0:
            data=re.match(r'^\s+(\d+)\s+(\d+)\s+(\d+).*$', output)
            byte=int(data.group(3))
            if byte>0:
                print "\nTask sucesses. Output information:"
                print "\tNumber of file:\t "+str(data.group(2))
                if byte>1024*1024*1024:
                    print "\tNumber of byte:\t %.2f GB"%(byte/(1024*1024*1024.0))
                elif byte>1024*1024:
                    print "\tNumber of byte:\t %.2f MB"%(byte/(1024*1024.0))
                elif byte>1024:
                    print "\tNumber of byte:\t %.2f KB"%(byte/1024.0)
                else:
                    print "\tNumber of byte:\t %d "%(byte)
            else:
                print "\nTask failed with output empty."
                self.alarm()
                return 1
        else:
            print "Task failed !!!"
            self.alarm()
        print
        return p.returncode

    def usage(self):
        print "Usage: python_program [-hsdr] "
        print "    -h --help        give this help"
        print "    -s --show        cat hadoop output"
        print "    -i --input       cat hadoop input"
        print "    -l --ls          ls hadoop output"
        print "    -d --debug       have to be followed by \"map\" or \"reduce\" "
        print "    -r --run         run hadoop program"
        print "    -p --hadoop      run hadoop program (internal use only)"

    def execute(self, argv):
        global strDate

        try:
            opts, args = getopt.getopt(argv[1:], "hd:s:ilr:p:", ["help", "debug=", "show=", "input", "ls", "run=", "hadoop="])
        except getopt.error, msg:
            print msg
            print "for help use --help"
            sys.exit(2)
        # process options
        if len(opts)==0:
            self.usage()
            sys.exit(0)
        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                sys.exit(0)
            elif o in ("-d", "--debug"):
                if a=="map":
                    # debug the map program
                    self.debugMap()
                elif a=="reduce":
                    self.debugReduce()
            elif o in ("-s", "--show"):
                strDate=a
                self.show()
            elif o in ("-i", "--input"):
                self.showInput()
            elif o in ("-l", "--ls"):
                self.ls()
            elif o in ("-r", "--run"):
                strDate=a
                rslt=self.run()
                sys.exit(rslt)
            elif o in ("-p", "--hadoop"):
                if re.match("^map",a)!=None:
                    if self.hasMapArgs:
                        self.mapper(self.argv)
                    else:
                        self.mapper()
                elif re.match("^reduce",a)!=None:
                    if self.hasReduceArgs:
                        self.reducer(self.argv)
                    else:
                        self.reducer()
            else:
                continue

