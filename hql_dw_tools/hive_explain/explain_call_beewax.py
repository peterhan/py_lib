#!coding:utf8
import requests
import json
import glob
import sqlparse
from explain_parse import parse_explain_string
import logging
logging.basicConfig(level=logging.DEBUG)


import urllib

def make_setting(setting_st):
    dic={}
    for line in setting_st.splitlines():
        line= line.strip(';')
        line=line.split(' ',1)[-1]
        arr=line.split('=')
        print arr
        key = arr[0].strip()
        value = arr[1].strip()
        dic[key]= value
    rdic={}
    for i,pair in enumerate(dic.items()):
        k,v=pair
        rdic['settings-%s-key'%i]=k
        rdic['settings-%s-value'%i]=v
    print rdic
    return rdic

def call_beewax_explain_query(query,cookies,setting_st,title='sub_statement'):
    
    query=query.decode('utf8')
    # print '[query]: ',query
    data={
    'query-query':query
    ,'query-database':'ad'
    ,'settings-next_form_id':0
    ,'file_resources-next_form_id':0
    ,'functions-next_form_id':0
    ,'query-email_notify':False
    ,'query-is_parameterized':True
    
    }
    data.update(make_setting(setting_st))
    # print requests.models.RequestEncodingMixin._encode_params(data)
    headers = {'X-CSRFToken':cookies['csrftoken'],"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    response=requests.post(url,data,cookies =cookies,headers=headers)
    try:
        jo=response.json()
    except Exception as e:
        print e,response.text
        print 'need login to get cookies'
        exit(255)        
    print json.dumps(jo,indent=2),'\n'*2
    if jo['status']!=0:
        print jo
        return
    exp=jo['explanation']
    print exp.encode('gbk','ignore'),'\n'*2
    parse_explain_string(exp,title)
    
# call_beewax_explain_query(querys,cookies)
# exit()

def explain_hql_query(querys,cookies,setting_st):    
    querys=sqlparse.parse(querys,'utf8')    
    for i,query in enumerate(querys):
        query_s=query.value.encode('utf8')
        if query_s.strip()=='':
            continue
        if query_s.startswith('set '):
            continue        
        call_beewax_explain_query(query_s,cookies,setting_st,'sub-%s'%i)
        
def explain_hql_file(fname,cookies,setting_st):
    # print fname
    querys=open(fname,'rb').read().decode('utf8')
    explain_hql_query(querys,cookies,setting_st)       

if __name__=='__main__':
    conf=json.load(open('conf.json'))

    url=conf['url']
    sessionid=conf['sessionid']
    csrftoken=conf['csrftoken']

    setting_st='''\
    set hive.cbo.enable=true;
    set hive.auto.convert.join=true;
    set hive.auto.convert.join.noconditionaltask = true;
    set hive.auto.convert.join.noconditionaltask.size = 10000;
    set hive.auto.convert.join.use.nonstaged=true; '''

    cookies ={'sessionid':sessionid,'csrftoken':csrftoken}
    path=conf['dir']
    # path='exp_query.sql'
    # explain_hql_query(querys)
    for fname in glob.glob(path+'/*'):
        print 'Explain File: '+fname
        explain_hql_file(fname.replace('\\','/') ,cookies,setting_st)