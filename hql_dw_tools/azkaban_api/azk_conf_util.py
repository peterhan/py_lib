import datetime

def get_ts():    
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
def parse_conf_file(ctnt):        
    vlu_dic={}
    line_list=[]
    for l in ctnt.splitlines():      
        if l.find('=')!=-1 and not l.startswith('#'):
            key,vlu=l.strip('\n').split('=',1)
            vlu_dic[key]=vlu
            line_list.append(key+' = ')
        else:
            line_list.append(l.strip('\n'))
    return {'list':line_list,'vlu_map':vlu_dic}
    
def generate_conf_file(parsed_obj):
    st=''
    for entry in parsed_obj['list']:
        if entry.endswith(' = '):
            key=entry[:-3]
            line=key+'='+parsed_obj['vlu_map'][key]
            st+=line+'\r\n'            
        else:
            st+=entry+'\r\n'            
    st+='#Modified by py conf_util.py \n'
    st+='#[%s] '%get_ts()    
    return st        
        
      
if __name__=='__main__':
    from conf import *
    st=open('env.properties').read()
    pobj= parse_conf_file(st)
    kv = pobj['vlu_map']
    # print kv.keys()
    print kv['failureEmails']
    print kv['failureSms']
    print kv['failureDingding']
    kv['failureDingding']=GT_email
    kv['failureSms']=GT_sms
    kv['failureEmails']=GT_email
    st=generate_conf_file(pobj)
    print st
