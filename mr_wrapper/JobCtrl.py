import os
import requests
from bs4 import BeautifulSoup

import datetime
def get_date(**kwargs):
    dateobj = datetime.datetime.now() + datetime.timedelta( **kwargs)
    return dateobj

HDP='/home/work/hadoop-client-stoff/hadoop/bin/hadoop '

def strip_text(nodes):
    return  map( lambda node:node.text, nodes )
    
def get_jobs_info(queuename):
    req = requests.get('http://szwg-stoff-abaci.dmop.baidu.com:8030/jobqueue.jsp?queueName=%s'%queuename)
    html = req.text.decode('utf8')
    soup = BeautifulSoup(html)
    job_info = {}    
    for tr in soup.find_all("tr"):
        if len(tr.find_all('td')) > 3:            
            nodes = tr.find_all('td', recursive=False)
            row = strip_text(nodes)
            job_info[row[0]]=row
        elif len(tr.find_all('th')) > 3:
            nodes = tr.find_all('th', recursive=False)
            header = strip_text(nodes)
        else:
            continue
    return job_info, header
        
def select_job_id( job_info, condition):
    ids={}
    sp_ids={}
    for id,row in job_info.items():
        if condition(row,sp_ids) :
            ids[id]=row
    return ids,sp_ids
    
def get_job_details( job_infos):
    n_job_infos = {}
    for jobid,row in job_infos.items():
        master=row[16]
        if master != 'null:0:0':                 
            master_list = master.split(':')
            master = master_list[0] + ':' + master_list[2]            
            jobxmlurl = 'http://%s/jobconf.jsp?jobid=%s'%(master,jobid)
            # print jobxmlurl;  exit()
            xml_dict = {}
            try:
                req = requests.get(jobxmlurl)
                html = req.text
                soup = BeautifulSoup(html)
                for tr in  soup.find_all('tr'):
                    if len(tr.find_all('td')) !=2:
                        continue
                    nodes = tr.find_all('td', recursive=False)
                    arr = strip_text(nodes)
                    xml_dict[ arr[0] ]=arr[1]
                row.append(xml_dict)
            except Exception ,e:
                print e
                pass
        n_job_infos[jobid] = row
    return n_job_infos

def print_header(header):
    for i,fd in enumerate( header): 
        print i,fd,',',
        if i%6==5: print ''
    print ''
    
def print_info(job_info):
    global header
    print '\t'.join(header)
    for row in job_info.values():
        print '\t'.join(row)
        
def exe_cmds(cmds):
    print '\n'.join(cmds)
    for cmd in cmds:
        os.system(cmd)
###
def setpriority(job_info,level):
    cmds=[]
    for jobid in job_info.keys():
        name = job_info[jobid][4]
        st = HDP+" job -set-priority %s %s #%s"% (jobid,level,name)
        cmds.append( st)
    exe_cmds(cmds)
    return cmds
    
###
def setcapacity(job_info):
    cmds=[]
    for jobid in job_info.keys():
        name = job_info[jobid][4]
        st = HDP+" job -set-map-capacity %s %s #%s"% (jobid,500,name)
        cmds.append( st)
        st = HDP+" job -set-reduce-capacity %s %s #%s"% (jobid,200,name)
        cmds.append( st)
    exe_cmds(cmds)
    return cmds
###    
def killjob(job_info):
    cmds=[]
    for jobid in job_info.keys():
        name = job_info[jobid][4]
        st = HDP+" job -kill %s  #%s"% (jobid,name)
        cmds.append( st)
    
    exe_cmds(cmds)
    return cmds
        
def parse_per(st):
    return float( st.strip('%') )*0.01

def stwith_any(st,list):
    for s in list:
        if st.startswith(s):
            return True
    return False    

header=[]
white=['hanshu','dengwei','wangyan','hanhan']
def lower_priority(all_job_info):
    def get_VH_job_id(row,sp_ids):
        if stwith_any(row[4],white):return False
        if row[2]!='rank-ubs': return False
        if row[1].find('VERY_HIGH')==-1:  return False
        map_per=parse_per(row[5])
        rdc_per=parse_per(row[9])
        if int(row[10])>1500 and rdc_per>0.8:
            print 'Anti deadlock:%s'%row
            sp_ids[row[0]]=row
            return False
        return True     
    ###
    print '### Set priority to High'
    job_info,sp_info = select_job_id(all_job_info,get_VH_job_id)
    # job_info = get_job_details(sp_info)    
    setpriority(job_info,'HIGH')
    
    
def lower_over_quota(all_job_info):
    ###
    def get_Mass_user(row,spids):        
        if stwith_any(row[4],white):return False
        if row[2]!='rank-ubs': return False
        if int(row[6])<1000 and int(row[10])<400:
            return False
        job_info=get_job_details( {row[0]:row} )
        # print row
        dc = row[-1] #detail data
        if type(dc)!=dict:
            return True
        mcap=int( dc['mapred.job.map.capacity'] )
        rcap=int( dc['mapred.job.reduce.capacity'] )
        if mcap>1000 or rcap>400:
            return True
    ###
    job_info,sp_info = select_job_id(all_job_info,get_Mass_user)
    print '#Quota task %s'%('\n#kill task '.join(job_info.keys()))
    setcapacity(job_info)    
    # killjob(job_info)    
    
def main():
    global header
    hour= get_date().strftime('%H%M')
    print int(hour)
    queuename='stoff-ps-ubs-off'
    all_job_info, header = get_jobs_info(queuename)
    # print_header(header)    
    # print all_job_info
    if int(hour)<=930 and int(hour)>=630:
        lower_priority(all_job_info)
    lower_over_quota(all_job_info)

if __name__ == "__main__":
    main()
    

'''
0 JobID , 1 Priority , 2 User , 3 Queue , 4 Job Name , 5 Map % Complete , 
6 Map Total , 7 Map Completed , 8 Map Running , 9 Reduce % Complete , 10 Reduce Total , 11 Reduce Completed , 
12 Reduce Running , 13 State , 14 Error , 15 FailOver Error Times , 16 Master , 17 Client IP , 
18 Last Update ,
''' 

