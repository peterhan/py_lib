#!coding:utf8
import re
import pdb
import json
import datetime
import StringIO
import sqlite3

import AzkabanAPI

def abbrev_name(proj,flow,job=None):
    if job is None:
        if flow.startswith(proj):
            return flow
        else:
            return '%s.%s'%(proj,flow)
    else:
        if job.startswith(proj):
            return '~'+job[len(proj)+1:]
        else:
            return job

def abbrev_full_name(pfj_arr):
    if pfj_arr.count('.')!=2:
        return pfj_arr
    proj,flow,job =pfj_arr.split('.')
    pf_st=abbrev_name(proj,flow)
    job=abbrev_name(proj,'',job)
    return pf_st+'.'+job

def load_job_info(job_st):
    job_info = {}
    WS=re.compile('\s+')
    for l in job_st.splitlines():
        row = WS.split(l,3)
        project,flow,job = row[:3]
        flow_name = '%s.%s'%(project,flow)
        job_info.setdefault(flow_name,[])
        job_info[flow_name].append(job)    
    return job_info
    
def make_message(notify_rows,short_message=True):
    if len(notify_rows)==0:
        return None
    else:
        collector={}
        for row in notify_rows:
            exid,proj,flow,job,status,info,dur,start_time=row[0:8]            
            if isinstance(status,unicode):
                status=status.encode('utf8')
            start_time=start_time.split(' ')[0]
            #print '\t'.join([str(exid),proj,flow,job,status,info,start_time])
            flow =  abbrev_name(proj,flow)
            job = abbrev_name(proj,'',job)
            id=u' [流:%s@%s#%s]'%(flow,start_time,exid)
            collector.setdefault(status,{})
            collector[status].setdefault(id,[])
            if len(info)>0:
                #print  job + info  
                if not short_message:
                    collector[status][id].append('%s:"%s"'%(job,info))
                else:
                    collector[status][id].append(job)
            else:
                collector[status][id].append(job)
    st=StringIO.StringIO()
    for status in reversed(collector.keys()):
        print>>st, u'[状态:{0}]:'.format(status)
        for id in collector[status]:
            jobs=',\n   '.join(collector[status][id])
            try:
                print>>st, u'{0}:\n  任务:{1}'.format(id,jobs)
            except Exception as e:
                print e
                print>>st, u'{0}:[{1}]'.format(id,jobs.encode('utf8','ignore'))
        print>>st, ''
    st=st.getvalue()
    return st
    
def calc_duration(start,end):
    fmt = AzkabanAPI.AzkabanAPI.time_FMT
    if start =='Unknown'  or end =='Unknown' :
        return u'Unknown' 
    if start is None  or end is None:
        return u'Unknown' 
    start = datetime.datetime.strptime(start,fmt)
    end = datetime.datetime.strptime(end,fmt)
    return unicode(str(end-start))
    
class AzkAPIResultParser:
    
    def __init__(self):
        self.DEP_WAIT_PAT=re.compile('.*\s\[?([\w_]+)\.([\w_]+)\.([\w_]+)(?:\.[\w_]+?\])?.*')
        self.STAGE_WAIT_PAT=re.compile('.*time\="(.*)" level\=".*" msg\="table ([\w\d_]*) is not sync.*')
        self.MRWAIT_PAT=re.compile('.*flow.dt.*:\[(\d{4}-\d{2}-\d{2})\].*')    
        self.MR_PAT=re.compile('.*\s(Stage-\d+ map = \d+%,  reduce = \d+%).*')    
        self.MR_STARTING_PAT=re.compile('.*-kill (job_[\d_]*).*')
        self.header='execid,project,flow,job,status,info,duration,start_time,end_time,update_time'
    
    def get_waitfor_flow_from_log(self,line):
        '''parse running log to find wait status
            return None if not wait
        '''
        runjob= line.split(' ')[3]
        #if line.find('sync')!=-1: pdb.set_trace()
        dep_keyword='依赖'.decode('utf8')
        m=self.DEP_WAIT_PAT.match(line)
        if line.find(dep_keyword)!=-1 and m is not None:
            st = '.'.join( m.groups())
            if st.startswith('mapreduce.') or st.startswith('hive.'):            
                return None 
            return st
        m=self.STAGE_WAIT_PAT.match(line)
        if m is not None:
            st =  m.groups()[1]
            time =  m.groups()[0]
            return st
        else:
            return None
    
    def get_mr_progress_from_log(self,line):
        '''parse running log to find run status
            try to compress info on fail just return log
        '''
        m=self.MR_PAT.match(line)
        if m:
            st = '.'.join( m.groups())
            return 'MR_Run:'+st
        m= self.MRWAIT_PAT.match(line)
        if m:
            st = '.'.join( m.groups())
            return "MR_flow.dt@%s"%st
        m= self.MR_STARTING_PAT.match(line)
        if m:
            st = '.'.join( m.groups())
            return u"MR_Init@JobId:%s"%st
        else:
            return None
    
    def parse_log_for_running_jobs(self,running,tail_num=5):
        '''parse running job 's log to find it is wait or running 
        '''
        s_running = {}
        for job_id,job_info in running.items():
            # print job_id,job_info
            log = job_info['logs'][-1]
            info = self.get_waitfor_flow_from_log(log)
            short_info = ''
            if info is not None:
                status=u'WAIT_DATA'
                info = 'Wait@%s'%abbrev_full_name(info)
            else:
                info = self.get_mr_progress_from_log(log)
                if info!=None:
                    status=u'RUN_MR'
                else:
                    status=u'RUN'
                    info='Log:'+log[23:40]
            job_info.update({'status':status,'info':info} )
            job_info.pop('logs')
            s_running[job_id]= job_info
        return s_running
        
    def summarise_to_rows(self,exec_detail):
        '''parse one executions result to rows
        '''
        st=''
        execid=exec_detail['execid']
        project=exec_detail['project']
        flow=exec_detail['flow']
        rows = []
        
        all_items=exec_detail['not_running'].items() + exec_detail['running'].items()
        for job,detail in all_items:            
            start=detail.get('start_time',u'Unknown')
            end=detail.get('end_time',u'Unknown')
            update=detail.get('update_time',u'Unknown')
            duration=calc_duration(start,end)            
            info=detail.get('info',u'')
            detail['status'] = detail['status']
            row=[execid, project,flow,job,detail['status'],info,duration,start,end ,update]
            rows.append(row)
        return rows
    
    def fetch_exec_detail_as_rows(self,azkaban_api,execid):
        exec_detail = azkaban_api.fetch_exec_detail(execid)
        exec_detail['running']=self.parse_log_for_running_jobs(exec_detail['running'])
        rows = self.summarise_to_rows(exec_detail)
        return rows
    
    def fetch_execs_detail_as_rows(self,azkaban_api,exec_ids):
        all_rows=[]
        for  exec_id in exec_ids:        
            rows = self.fetch_exec_detail_as_rows(azkaban_api,exec_id)                
            all_rows.extend(rows)
        return all_rows
        
    def fetch_execs_detail_as_sqlite(self,azkaban_api,exec_ids,sqlitefile=None,account='dw'):        
        if not sqlitefile:
            conn=sqlite3.connect(':memory:')
        else:
            conn=sqlite3.connect(sqlitefile)
        cols = self.header.split(',')
        cols_define = ',\n'.join(['%s string'%col for col in cols])
        cols_holder = ','.join(['?' for col in cols])
        sql_ddl = '''create table if not exists 
        azk_status(   
            account string, execid string ,
            project string, flow string,
            job string, status string,
            info string, duration string,
            start_time string,  end_time string,
            update_time string,
            PRIMARY KEY (account,execid,project,flow,job)
            );    '''
        # print sql_ddl
        conn.execute(sql_ddl)
        sql_insert = "Replace into azk_status values('%s',%s);"%(account,cols_holder)        
        rows = self.fetch_execs_detail_as_rows(azkaban_api,exec_ids)
        print len(rows)
        conn.executemany(sql_insert,rows)
        conn.commit()
        return conn
        

class AzkAPIResultParser_DataClean(AzkAPIResultParser):

    def data_clean_logs_parse(self,logs):
        tag=['dcl_log====program_start',
        'dcl_log====dcl_tag_wait' ,
        'dcl_log====dcl_execute_error' ]
        status='RUN'
        if logs[-1].find('dcl_log====dcl_tag_wait')!=-1:
            status=u'程序在等待标记'
        if logs[-1].find('msg="table ')!=-1:
            status=u'Stage_Wait'
        if any(map(lambda line:line.find(tag[2])!=-1, logs)):
            status=u'程序错误'
        #info=logs[-1]
        info=''
        return status,info
    
    def parse_log_for_running_jobs(self,running,tail_num=5):
        '''parse running job 's log to find it is wait or running 
        '''
        s_running = {}
        for job_id,job_info in running.items():
            status,info=self.data_clean_logs_parse(job_info['logs'])
            job_info.update({'status':status,'info':info} )
            job_info.pop('logs')
            s_running[job_id]= job_info
        return s_running

def is_night():
    fmt='%H'
    now=int(datetime.datetime.now().strftime(fmt))
    print now
    if now >=22 or now <=6:
        return True
    return False

def is_today(time_s):
    fmt = AzkabanAPI.AzkabanAPI.time_FMT
    now=datetime.datetime.now()
    ts=datetime.datetime.strptime(time_s,fmt)
    delta=datetime.timedelta(days=1)
    print 'is_today',now-ts,delta
    if abs(now-ts) <delta:
        return True
    return False

def is_thishour(time_s):
    fmt = AzkabanAPI.AzkabanAPI.time_FMT
    now=datetime.datetime.now()
    ts=datetime.datetime.strptime(time_s,fmt)
    delta=datetime.timedelta(hours=1)
    print 'is_thishour',now,ts,now-ts,delta
    if abs(now-ts) <delta:
        return True
    return False

def make_end_info(exec_ids):
    end_info=''
    for k,info in exec_ids.items():
        dur = calc_duration(info['start_time'],info['end_time'])
        flowid = info['flowId']
        proj = info['project']
        flowid=abbrev_name(proj,flowid)
        end_info+= '[%s]Start@%s\n'%(flowid,info['start_time'][:16].replace(' ','_'))    
    end_info = end_info.encode('utf8')
    return end_info

def test_dw(aa):
    aarp=AzkAPIResultParser()
    exec_ids = aa.get_exec_ids(topn=2)
    rows = aarp.fetch_execs_detail_as_rows(aa,exec_ids)        
    for row in  rows:        
        print str(row[0])+'\t'+'\t'.join(row[1:]).encode('utf8')
    print make_message(rows)

def test_dc(aa):
    '''data clean parser test'''
    aarp=AzkAPIResultParser_DataClean()
    exec_ids = aa.get_exec_ids(topn=1)
    rows = aarp.fetch_execs_detail_as_rows(aa,exec_ids)
    for row in  rows:        
        print str(row[0])+'\t'+'\t'.join(row[1:]).encode('utf8')
    print make_message(rows)

def test_sqlite(aa):
    aa_helper = AzkAPIResultParser()
    exec_ids = aa.get_exec_ids(topn=5)    
    sqlf='temp.sqlite'
    conn = aa_helper.fetch_execs_detail_as_sqlite(aa,exec_ids,sqlf,'dw')    
    # conn = sqlite3.connect(sqlf)
    cur = conn.execute('select * from azk_status;')
    for row in cur:
        print row
    
def test_util(aa):        
    ts='2017-02-17 11:11:11'
    print is_today(ts)
    print is_thishour(ts)
    print is_night()

def test_prep():
    aa=AzkabanAPI.AzkabanAPI('http://scheduler.data.com.cn:8080')    
    aa.login(username='username',password='password')
    return aa

if __name__=='__main__':
    aa=test_prep()
    # test_dw(aa)
    # test_dc(aa)
    # test_util(aa)
    test_sqlite(aa)
