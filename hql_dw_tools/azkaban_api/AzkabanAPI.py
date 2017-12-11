#!coding:utf8
import requests
import re
import HTMLParser
import logging
import json
import datetime
import pdb
import os

### log setting start
FORMAT='%(asctime)-10s %(message)s'
# logging.basicConfig( level=logging.DEBUG, format=FORMAT ,filemode='a',filename='azk_api.log')
logger=logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
hdlr = logging.StreamHandler()
hdlr.setLevel(logging.INFO)
sformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
hdlr.setFormatter(sformatter)
# logger.addHandler(hdlr)
### log setting end

### https warning off start
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning,SNIMissingWarning,InsecurePlatformWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    requests.packages.urllib3.disable_warnings(SNIMissingWarning)
    requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
except:
    pass

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += 'HIGH:!DH:!aNULL'
try:
    requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += 'HIGH:!DH:!aNULL'
except AttributeError:
    # no pyopenssl support used / needed / available
    pass
### https warning off end

    
class AzkabanAPI():
    time_FMT='%Y-%m-%d %H:%M:%S'
    def __init__(self,host):
        self.host=host
        self.project=None
        self.session_id=None
        self.controller_map = {'exe':'executor','mgr':'manager','schd':'schedule'}
        self.cmd_map={
            'exe':{
                'gr':'getRunning',    'fef':'fetchexecflow',
                'pf':'pauseFlow',     'cf':'cancelFlow',
                'sf':'scheduleFlow',  'usf':'removeSched',
                'ef':'executeFlow',   'rf':'resumeFlow',
                'fejl':'fetchExecJobLogs',  'fi':'flowInfo',
                'fefu':'fetchexecflowupdate'
                },
            'mgr':{
                'fpf':'fetchprojectflows',
                'ffg':'fetchflowgraph',
                'ffe':'fetchFlowExecutions'
                },
            'schd':{
                'fs':'fetchSchedule',
                'scf':'scheduleCronFlow',
                'sf':'scheduleFlow'
            }            
        }
    ## util function
    def repr_of_list(self,lst):
        content='","'.join(lst)
        return '["%s"]'%content
        
    def select_dict(self,dic,keys):
        ndic={}
        for key in keys:
            ndic[key]=dic[key]
        return ndic

    def format_period(self,period):
        plist= period.split()
        unit=plist[1][0].lower()
        if unit=='m':#'big M for month'
            unit='M'
        n_period=plist[0]+''+unit
        return n_period
        
    def format_schd_date(self,sched_time):
        dobj=datetime.datetime.strptime(sched_time,'%Y-%m-%d %H:%M:%S')
        # print dobj.strftime('%m/%d/%Y')
        return dobj.strftime('%m/%d/%Y'),dobj.strftime('%I,%M,%p,+08:00')
    
    def set_project(self,project):    
        self.project=project    
       
    def unix2ts(self,unix_timestamp,base=10):
        try:
            if unix_timestamp==-1:
                return u'Unknown'
            ts=datetime.datetime.fromtimestamp( 
                 #int(unix_timestamp,base)/1000 
                 unix_timestamp/1000 
                ).strftime(AzkabanAPI.time_FMT)
            return unicode(ts)
        except:
            # print 'ts Exception',unix_timestamp
            return None
        
    def add_common_fields(self,data):
        '''add common field for azkaban request'''
        cdata=data
        if not 'session.id' in data:
            cdata['session.id']=self.session_id
        if not 'project' in data:
            cdata['project']=self.project
        return cdata
        
    def parse_response(self,response,url,data,is_html=False):
        '''parse https response to json or text
        '''
        content_type=response.headers.get('Content-Type','')
        if content_type.find('application/json')!=-1:        
            jo = response.json()
            return jo
        elif content_type.find('application/zip')!=-1:
            return response
        else:
            if not is_html:
                print url,data
                print response.headers
                print response.text
                logging.debug( "[Can't parse to json response] %s",response.text)
            return response  
    
    def get(self,url,data,is_post=False,is_html=False):
        '''HTTP get azkaban 's api data '''
        data = self.add_common_fields(data)
        logger.debug( '[Request get data] :%s, %s',url,data)
        if is_post:
            response=requests.post(self.host+url,data, verify=False)        
        else:
            response = requests.get(self.host+url,data, verify=False)
        return self.parse_response(response,url,data,is_html)
        
    def post_with_file(self,url,data,files):
        response=requests.post(self.host+url,data,files=files, verify=False)
        return self.parse_response(response,url,data)
        
    def login(self,username='',password=''):
        '''login the azkaban'''
        data = {'action':'login','username':username,'password':password}
        response = self.get('/?action=login',data,is_post=True)
        logger.debug('[Login]:%s',response) 
        #print 'login response:',response.text
        self.session_id = response['session.id']        
        logger.debug('[Login success]:%s',self.session_id)
        return self.session_id
        
    def fetch_project_list(self):    
        '''get all project list'''
        data={'session.id':self.session_id}
        projs=[]
        pat=re.compile(r'"/manager\?project=(\w*)"')
        response = self.get('/index',data,is_html=True)
        logger.debug('[Fetch_project_list]html_len:%s', len(response.text))
        for line in response.text.split('\n'):
            matched=pat.search(line)
            if matched:
                proj=matched.group(1)
                # print proj
                if len(proj)==0:
                    continue
                projs.append(proj) 
        return projs   
    
    def upload_project(self,project,filename,safekey=''):
        '''上传项目
        '''
        if safekey!='dwdwdw':
            return {'Error':'Upload safekey is wrong, see the AzkabanAPI upload_project source ...'}
        data={'project':project,'ajax':'upload','session.id':self.session_id}
        fname=os.path.split(filename)[-1]
        files={'file':(fname, open(filename,'rb').read(),'application/zip',{'Expires': '0'})}
        response = self.post_with_file('/manager',data,files)        
        return response    
    
    def download_project(self,project,folder='data'):
        
        if not os.path.exists(folder):os.mkdir(folder)
        data={'project':project}
        response=self.get('/manager?download=true',data)
        with open('%s/%s.zip'%(folder,project),'wb') as fh:
            fh.write(response.content)
        return response
        
    def print_cmd_map(self):                
        print json.dumps(self.controller_map,indent=2)
        print json.dumps(self.cmd_map,indent=2)
        return self.controller_map,self.cmd_map
        
    def ajax_cmd(self,controller,cmd_option, **idata):
        '''send executor's  command get json data
        '''
        #flow=None,project=None,execid=None,idata=None,start=0,length=5,is_post=False
        if len(cmd_option)<5:
            cmd = self.cmd_map.get(controller,{}).get(cmd_option,'not_found')
        else:
            cmd=cmd_option
        data={'ajax':cmd,}
        controller = self.controller_map.get(controller,'')
        if cmd=='fetchFlowExecutions':
            data.setdefault('start',0)
            data.setdefault('length',5)
        if self.project!=None:
            data['project']=self.project
        data.update(idata)
        logger.debug('[%s called:] %s %s %s',controller,cmd,data)        
        is_post=idata.get('is_post',False)
        response=self.get('/%s'%controller,data,is_post=is_post)
        return response
        
    def fetch_flows(self,project):
        '''fetch project's flows
        '''
        res = self.ajax_cmd('mgr','fetchprojectflows',project=project)
        # print res.text,res.json()
        flowids = [ r['flowId'] for r in res['flows'] ] 
        return flowids

    def fetch_flow_graph(self,flow,project=None):
        '''fetch flow 's job graph
        '''
        if not project:
            graph = self.ajax_cmd('mgr','fetchflowgraph',flow=flow,project=self.project)
        else:
            graph = self.ajax_cmd('mgr','fetchflowgraph',flow=flow,project=project)
        if not isinstance(graph,dict) and len(graph.text)==0:
            logger.debug( '[Not found flow] %s',self.host)
            return False
        return graph
    
    def get_project_flow_job(self):
        '''get project-flow-job data '''
        all_projects = self.fetch_project_list()
        all_flows={}
        # print 'all_projects:',len(all_projects)
        for project in all_projects:
            flows=self.fetch_flows(project)
            # print project,flows
            for flow in flows:
                flowgraph = self.fetch_flow_graph(flow,project=project)
                key = '%s.%s'%(project,flow)
                if key in all_flows:
                    print 'duplicate flow:',key
                all_flows[key] = flowgraph
                # print json.dumps(flowgraph,indent=2)
        return all_flows

    def fetch_flow_exec_info(self,flow,topn=1):
        ''' Fetch history executions,need set project first
            return dict key:execid , value: exec_basic_info
        '''
        history_info=[]
        # project flow combine format
        if flow.find('.')!=-1:
            project,flow=flow.split('.',1)                        
            response = self.ajax_cmd('mgr','fetchFlowExecutions',flow=flow,project=project,start=0,length=10000)
        # only flow ,project is set before calling
        else:
            response = self.ajax_cmd('mgr','fetchFlowExecutions',flow=flow,start=0,length=10000)
            project=self.project
        execs={}
        #print json.dumps(response)
        #pdb.set_trace()
        if ('executions' not in response) or len( response['executions'] )==0:
            return {}
        for node in response['executions']:
            exec_id = int(node.get('execId'))            
            stime = self.unix2ts( node.get('startTime'))
            etime = self.unix2ts( node.get('endTime'))
            subtime = self.unix2ts( node.get('submitTime'))
            node.update({'project':project,'start_time':stime,'end_time':etime,'submit_time':subtime})
            execs[exec_id] = node
        exec_ids = [exec_id for exec_id in execs ]
        ## get last N record
        #pdb.set_trace()
        exec_ids = sorted(exec_ids,reverse=True)[:topn]
        exec_info={}
        for id in exec_ids:
            exec_info[id]=execs[id]
        return exec_info
        
    def get_project_id(self,project):
        flows=self.fetch_flows(project)
        if len(flows)==0:
            return None
        jo= self.fetch_flow_graph(flows[0],project)
        # print jo
        return jo.get('projectId')
        
    def make_formated_time_patch(self,flow_info,keys,old_suffix='Time',new_suffix='_time'):
        '''make a new time dict to update old data struct
        '''
        time_patch={}
        for key in keys.split(','):
            unix_time = flow_info.get(key + old_suffix,0)
            time_patch[key + new_suffix] = self.unix2ts(unix_time)        
        return time_patch
    
    def get_exec_ids(self,proj_flow=None,topn=1):
        '''
        pass proj_flow to return topn exec_id else return last one
        '''
        exec_ids={}
        ##'get all execid'
        if not proj_flow:
            porjects = self.fetch_project_list()
            for porject in porjects:
                flows = self.fetch_flows(porject)
                for flow in flows:
                    proj_flow=porject+'.'+flow
                    exec_info=self.fetch_flow_exec_info(proj_flow,topn=topn)
                    for eid,einfo in exec_info.items():
                        exec_ids[eid]=einfo
        else:
            exec_info=self.fetch_flow_exec_info(proj_flow,topn=topn)
            for eid,einfo in exec_info.items():
                exec_ids[eid]=einfo
        return exec_ids
        
    def fetch_exec_detail(self,execid,get_all_log=False,tail_num=-51):
        '''get executions detail ,
            get_all_log :is get finish/error jobs' log
            tail_num :keep log length
        '''
        flow_info = self.ajax_cmd('exe','fetchexecflow',execid=execid)
        hp = HTMLParser.HTMLParser()
        # print '[Dump fetchexecflow]:'
        # print json.dumps(flow_info,indent=2)
        if 'nodes' not in flow_info:
            logger.error('[fetch_exec_detail fail]:%s,%s',execid,flow_info)
            print '[Error fetch_exec_detail fail]:%s,%s',execid, flow_info
            return {}
        exec_detail = {
            'project':flow_info['project'],
            'flow':flow_info['flowId'],
            'execid':flow_info['execid'],
            'submitUser':flow_info['submitUser'],
            'attempt':flow_info['attempt'],
            'not_running':{},'running':{}
            }
        time_patch = self.make_formated_time_patch(flow_info,'start,end,update,submit')
        exec_detail.update(time_patch)
        ## retrive running logs from azkaban api
        for job in flow_info['nodes']:
            job_id=job['id']
            time_patch = self.make_formated_time_patch(job,'start,end,update')
            job.update(time_patch)
            if not get_all_log and job['status']!='RUNNING' :
                exec_detail['not_running'][job_id]= job
                continue                        
            response = self.ajax_cmd('exe','fetchExecJobLogs', execid=execid,jobId=job_id, offset=0, length=500000000, is_post=False)
            try:
                jo=response
            except Exception as e:
                print e
                logger.error( '[JSON parse ERROR]: %s %s',type(response),job['status'])
                continue            
            lines=[]
            for line in jo['data'].split('\n'):
                line=hp.unescape(line)
                if len(line)==0:
                    continue
                lines.append(line)
            job['logs']=lines[tail_num:]
            if job['status']=='RUNNING':
                exec_detail['running'][job_id]=job
            else:
                exec_detail['not_running'][job_id]=job
        # if len(exec_detail['running'].keys())>=1: print json.dumps(exec_detail,indent=2)
        return exec_detail
        
    def get_flow_exec_info(self, projectId, project, flow):
        '''get flow info for execute/schedule api'''        
        flow_schd_info = self.ajax_cmd('exe','flowInfo',project=project,flow=flow)     
        info = {}
        ## proj flow info
        info['project']=project
        info['projectId']=projectId
        info['projectName']=project
        info['flow']=flow
        ## flow_schd_info
        for key,value in flow_schd_info.items():
            if key in ('flowParam'):
                for subkey,subvalue in value.items():
                    key='flowOverride[%s]'%subkey
                    info[key]=subvalue
            # print key,value,type(value)
            elif key =='concurrentOptions': 
                info['concurrentOption']=value
            elif type(value)==list:
                info[key]=','.join(value)
            else:
                if value in (True,False):
                    value=str(value).lower()
                info[key]=value
        return info
        
    def execute_flow(self,project,flow,jobs,dt=None,projectId=None):
        '''execute project flow 's specify jobs'''
        if projectId is None:
            projectId=self.get_project_id(project)
        info=self.get_flow_exec_info( projectId, project, flow)
        flow_graph = self.fetch_flow_graph(flow,project)
        disabled= [node['id'] for node in flow_graph['nodes'] if node['id'] not in jobs]
        print '[RunJob]',jobs
        print '[DisabledJob]',disabled
        info['disabled']=self.repr_of_list(disabled)
        if dt !=None:
            info['flowOverride[flow.dt]']=dt
        logger.info('[Execute Flow]:%s, flow.dt:%s,Jobs:"%s" Diabled:"%s"',flow,dt,jobs,info['disabled'])
        res =self.ajax_cmd('exe','executeFlow',**info )
        res['status_page']=self.host+"/executor?execid=%s"%res['execid']
        return res
        
    ## util function    
    def complete_flow_schedule_info(self,sched_info):
        ''''reformat submit info for schedule api'''        
        info={}    
        info['disabled']='[]'
        info['is_recurring']='on'
        ## schedule info 
        sched_info=sched_info['schedule']
        info['period']=self.format_period(sched_info['period'])    
        dt_arr=self.format_schd_date(sched_info['firstSchedTime'])
        info['scheduleDate']=dt_arr[0]
        info['scheduleTime']=dt_arr[1]
        return info        

        
    def update_azkaban_schedule(self,project,flow,create_user,dry_run=False,projectId=None):
        '''update schedule default no action
        '''
        if projectId is None:
            projectId=self.get_project_id(project)
        proj_flow=project+'.'+flow
        logging.info( '[Flow to update prj_id:%s] %s',projectId,proj_flow)    
        sched_info = self.ajax_cmd('schd','fetchSchedule',projectId=projectId,flowId=flow)
        user =sched_info.get('schedule',{}).get('submitUser','no_sched_user')
        if user !=create_user:
            return {'error':'Create user is %s ,not %s'%(user,create_user)}
        if len(sched_info)==0:
            logging.info( '[!!!Current no schedule!!!]:%s',proj_flow)
            return {'error':'No schedule found'}    
        # prepare the api data
        update_sched_info = self.get_flow_exec_info(projectId, project, flow) 
        # schdule info
        sched_info = self.complete_flow_schedule_info( sched_info ) 
        update_sched_info.update(sched_info)    
        sched_essential = self.select_dict(update_sched_info,['period','scheduleDate','scheduleTime'])
        logging.info( '[Schedule Info]: %s,%s',proj_flow,sched_essential )    
        ### write back 
        if not dry_run:
            ## update azkaban
            logging.info('[Update schedule_info]')
            update_result = self.ajax_cmd('schd','scheduleFlow', **update_sched_info)    
        else:
            update_result = {'error':'Dry Run,Not update the Azkaban'}
        return update_result

        

def jsdump(prompt,obj):     
    s=json.dumps(obj,indent=2)
    print '\n###[%s]###'%prompt
    print s
    raw_input('[Press any key to continue]')
    return s
    
def test_all():
    aa = AzkabanAPI('http://scheduler.data.com.cn:8080/custom')
    aa.login(username='username', password='password')
    
    res=aa.update_azkaban_schedule('dw_test','dw_test_noop','dw')
    jsdump('update_azkaban_schedule',res)
    
    res=aa.fetch_project_list()
    jsdump('fetch_project_list',res)
    
    prj=res[0]
    res=aa.download_project(prj)
    jsdump('download_project',dict(res.headers))
    
    res=aa.get_project_flow_job()
    jsdump('get_project_flow_job',res.items()[0])
    
    res= aa.fetch_flows('dw_test')
    jsdump('fetch_flows',res)
    
    res=aa.fetch_flow_graph('dw_test_noop','dw_test')
    jsdump('fetch_flow_graph',res)
    
    res=test_execute(aa)
    jsdump('execute_flow',res)
    
    res=aa.get_exec_ids('dw_test.dw_test_noop',topn=5)
    jsdump('get_exec_ids',res)
    
    eid=res.keys()[4]
    print 'Fetch Execid: ',eid,res[eid]['status']
    res=aa.fetch_exec_detail(eid,get_all_log=True,tail_num=-5)
    jsdump('fetch_exec_detail',res)
    ## print all flow
    # print json.dumps(aa.get_project_flow_job())
    ## test ajax api mapping
    test_ajax_cmd(aa)
    
    
    return None
    
def test_execute(aa):
    project='dw_test'    
    flow='dw_test_noop'
    jobs=['test1']
    dt='2017-05-04'
    res= aa.execute_flow(project,flow,jobs,dt)    
    return res
    
def test_ajax_cmd(aa):
    '''scan code verify ajax call is valid'''
    omap,cmap=aa.print_cmd_map()
    for i,line in enumerate(open('AzkabanAPI.py')):
        if line.find('self.ajax_cmd')!=-1:
            arr= line.strip().split('_cmd(')
            if len(arr)<2:
                continue
            cmd_arr= arr[1].replace("'","").split(',')
            objcmd=cmap.get(cmd_arr[0],{})
            # print objcmd.values()
            print '[Test ajax cmd_setting]:LineNo:%s, %s, %s'%(i+1,cmd_arr[1],cmd_arr[1] in objcmd.values())
    return objcmd

if __name__=='__main__':
    test_all()    

