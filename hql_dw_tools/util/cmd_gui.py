#  -*- coding:utf8 -*-
import sys
import os
import datetime
import time
import glob

SLEEP_PAUSE = 1

    
def get_date(fmt='%Y-%m-%d',base=datetime.datetime.now(), isobj=False, **kwargs ):
    i_str2date=lambda str_date,fmt: datetime.datetime.fromtimestamp(time.mktime(time.strptime(str_date,fmt)))
    if isinstance(base,basestring):
        dateobj= i_str2date(base,fmt)+ datetime.timedelta( **kwargs)
    else:
        dateobj = base + datetime.timedelta( **kwargs)
    if isobj: 
        return dateobj
    else: 
        return dateobj.strftime(fmt)
        
    
def get_timestamp(fmt='%Y%m%d'):
    return datetime.datetime.now().strftime(fmt)
    
def pause(IS_INTERACTIVE=False):
    if IS_INTERACTIVE:
        raw_input('[press Enter to continue]')
    else:
        time.sleep(SLEEP_PAUSE)
        
def expand_dt(dt):
    # escape
    if dt is None:
        return dt
    # date list
    if ',' in dt:
        dt=dt.split(',')
    # date range
    if '=>' in dt:
        start,end=dt.split('=>',1)
        dt,edt,i=[],'',0        
        if start <= end:
            while edt<end:
                edt=get_date(base=start,days=i)
                dt.append(edt)
                i+=1
        else:
            edt=start
            while edt >= end:
                edt=get_date(base=start,days=-1*i)
                dt.append(edt)
                i+=1
    return dt
    
def get_common_prefix(strs):
    if len(strs)==1:
        return '',strs
    prefix=os.path.commonprefix(strs)
    ln=len(prefix)
    nstrs=[st.split('/')[-1] for st in strs]
    return prefix,nstrs

def print_table(seq, columns=2,common_prefix=None):
    if common_prefix:
        prefix,seq = get_common_prefix(seq)
        table = ' \n\n [Common prefix is:  %s] \n\n'%prefix
    else:
        table=''
    col_height = len(seq) / columns+1
    lpad=120/columns
    for x in xrange(col_height):
        for col in xrange(columns):
            pos = x+(col_height)*col
            try:
                num = seq[pos]
                # table += ('(%s) %s %s,%s' % (pos, num,x,col)).ljust(l_pad)
                table += ('(%s) %s ' % (pos, num)).ljust(lpad)
            except:
                pass
        table += '\n'
    return table.strip('\n')

def parse_id_list(choices):
    try:        
        choices = map(int, choices.replace(',',' ').split())
        is_list=True
        if len(choices)==1:
            choices=choices[0]
    except Exception as e:
        # print '[List Select Exception]:',e,type(e)
        is_list=False
    return is_list,choices
    
def select_choices_from_list(choices,candidate_list):
    is_list,choices=parse_id_list(choices)
    if is_list:
        idx_choices = []
        value_choices = []
        for e_choice in choices:            
            if e_choice<len(candidate_list):                
                idx_choices.append(e_choice)
                value_choices.append(candidate_list[e_choice])
            else:
                return 'out_of_bound',False
        return idx_choices,value_choices
    else:
        if choices.startswith('job:'):
            pass
        elif choices=='all':
            all_id=[]
            for i in range(len(candidate_list)):
                all_id.append(i)
            return all_id,candidate_list
        elif choices in ('q','quit'):
            print 'Thanks for use, Bye!'
            sys.exit(0)
            return 'quit',choices
        return 'not_in_list',choices
    
def get_user_select(prompt,candidate_list,print_colnum=1,common_prefix=None,sort_list=True):
    # return 'not_in_list',choice
    print '\n[%s]'%prompt
    if sort_list:
        candidate_list=sorted(candidate_list)
    print print_table(candidate_list,print_colnum,common_prefix)    
    print '["q"','to quit]'
    choice=raw_input('[Input Number]>>')    
    if len(candidate_list)==0:
        return 'empty_list',choice
    is_list,choice= parse_id_list(choice)
    if is_list:
        if choice<len(candidate_list):
            return 'succ_%s'%choice,candidate_list[choice]
        else:
            return 'out_of_bound',False
    else:
        if choice in ('q','quit'):
            print 'Thanks for use, Bye!'
            sys.exit(0)
            return 'quit',choice
        return 'not_in_list',choice

def get_user_select_list(prompt,candidate_list,print_colnum=1,common_prefix=None,sort_list=True):
    # return 'not_in_list',choices
    print '\n[%s]'%prompt
    candidate_list=sorted(candidate_list)
    print print_table(candidate_list,print_colnum,common_prefix)  
    print '["q"','to quit.]'
    choices = raw_input('[Input Multi Number]>>')  
    if len(candidate_list)==0:
        return 'empty_list',choices
    return select_choices_from_list(choices,candidate_list)
        
def get_job_name(job_path):
    return job_path.split('/')[-1].split('.')[0]
    
def get_user_select_job_list(prompt,candidate_list,print_colnum=1,common_prefix=None,sort_list=True):
    # return 'not_in_list',choices
    print '\n[%s]'%prompt
    candidate_list=sorted(candidate_list)
    short_list=map(get_job_name,candidate_list)
    print '[Full JobList ]:'
    for i,elm in enumerate(short_list):
        print elm+',',        
    filters = raw_input('\n[(Filter pattern split by ",",Empty will select all. )]\n[Filter Patterns]:')
    if filters=='':
        filters=None
    else:
        filters=map(lambda x:x.strip(),filters.split(','))
    # use filter to filter select job
    if filters is not None:
        new_candidate_list=[]
        candidate_list_dict=dict(zip(short_list,candidate_list))
        keys=candidate_list_dict.keys()
        keys_not_found=set(filters)-set(keys)
        found_keys=set(keys).intersection(filters)
        for key in found_keys:
            value=candidate_list_dict[key]
            print '[Matched pattern]:',value
            new_candidate_list.append(value)
        print '[Not found pattern]:',list(keys_not_found)
        candidate_list=new_candidate_list
    ## select job to operation
    print print_table(candidate_list,print_colnum,common_prefix)
    print '["q"','to quit.]'
    choices = raw_input('[Input Multi Number]>>')  
    if len(candidate_list)==0:
        return 'empty_list',choices
    ##
    return select_choices_from_list(choices,candidate_list)
        
        
def select_files(path,prompt='',suffix='txt'):
    ts=get_timestamp()   
    print '[%s]'%prompt
    path_pattern = os.path.join(path,'*%s*.%s'%(ts,suffix))
    pattern = raw_input('[File name pattern]:"%s"\n[Default:%s] or q to quit>>'%(path_pattern,ts))
    if pattern=='q':
        return None
    if len(pattern)==0:        
        pattern=ts
    path_pattern = os.path.join(path,'*%s*.%s'%(pattern,suffix))
    print '[List name match pattern]:',path_pattern
    matched_files=[]
    for matched_file in glob.glob(path_pattern):        
        matched_files.append(matched_file)
    print '[Matched files]:',matched_files
    ids,selected_files=get_user_select_list(prompt,matched_files,2)
    return ids,selected_files
    
if __name__=='__main__':
    print expand_dt('2017-01-01,2017-02-11')
    print expand_dt('2017-02-01=>2017-02-11')
    print expand_dt('2017-02-11=>2017-02-05')
    # print select_files('..\\sh\\.','Select shell file','sh')
    print get_user_select_job_list('test_filter_select',['a','b','c'],print_colnum=1,common_prefix='sh')
    