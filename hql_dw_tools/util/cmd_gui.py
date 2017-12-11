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
    if ',' in dt:
        dt=dt.split(',')
    if '=>' in dt:
        start,end=dt.split('=>',1)
        dt=[]
        edt=''
        i=0
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

def print_table(seq, columns=2,fold_str=False):
    if fold_str:
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

def get_user_select(prompt,alist,print_colnum=1,fold_str=False,sort_list=True):
    # return 'not_in_list',choice
    print '\n[%s]'%prompt
    if sort_list:
        alist=sorted(alist)
    print print_table(alist,print_colnum,fold_str)    
    print '["q"','to quit]'
    choice=raw_input('[Input Number]>>')    
    if len(alist)==0:
        return 'empty_list',choice
    try:
        choice=int(choice)
        if choice<len(alist):
            return 'succ_%s'%choice,alist[choice]
        else:
            return 'out_of_bound',False
    except:
        if choice in ('q','quit'):
            print 'Thanks for use, Bye!'
            sys.exit(0)
            return 'quit',choice
        return 'not_in_list',choice

def get_user_select_list(prompt,alist,print_colnum=1,fold_str=False,sort_list=True):
    # return 'not_in_list',choices
    print '\n[%s]'%prompt
    alist=sorted(alist)
    print print_table(alist,print_colnum,fold_str)  
    print '["q"','to quit.]'
    choices = raw_input('[Input Multi Number]>>')  
    if len(alist)==0:
        return 'empty_list',choices
    try:        
        choices = map(int, choices.replace(',',' ').split())
        ichoices = []
        rchoices = []
        for e_choice in choices:            
            if e_choice<len(alist):                
                ichoices.append(e_choice)
                rchoices.append(alist[e_choice])
            else:
                return 'out_of_bound',False
        return ichoices,rchoices
    except:
        if choices in ('q','quit'):
            print 'Thanks for use, Bye!'
            sys.exit(0)
            return 'quit',choices        
        return 'not_in_list',choices
        
def get_job_name(job_path):
    return job_path.split('/')[-1].split('.')[0]
    
def get_user_select_job_list(prompt,alist,print_colnum=1,fold_str=False,sort_list=True):
    # return 'not_in_list',choices
    print '\n[%s]'%prompt
    alist=sorted(alist)
    short_list=map(get_job_name,alist)
    print '[Unfiltered JobList ]:'
    for i,elm in enumerate(short_list):
        print elm+',',        
    filters = raw_input('[(default no filter, filter string split by ",")]\n[Filters Input]:')
    if filters=='':
        filters=None
    else:
        filters=map(lambda x:x.strip(),filters.split(','))
    # use filter to filter select job
    if filters is not None:
        new_alist=[]
        alist_dict=dict(zip(short_list,alist))
        keys=alist_dict.keys()
        keys_not_found=set(filters)-set(keys)
        found_keys=set(keys).intersection(filters)
        for key in found_keys:
            value=alist_dict[key]
            print '[Matched]:',value
            new_alist.append(value)
        print '[not found jobs]',list(keys_not_found)
        alist=new_alist
    print print_table(alist,print_colnum,fold_str)  
    print '["q"','to quit.]'
    choices = raw_input('[Input Multi Number]>>')  
    if len(alist)==0:
        return 'empty_list',choices    
    try:        
        choices = map(int, choices.replace(',',' ').split())
        ichoices = []
        rchoices = []
        for e_choice in choices:            
            if e_choice<len(alist):                
                ichoices.append(e_choice)
                rchoices.append(alist[e_choice])
            else:
                return 'out_of_bound',False
        return ichoices,rchoices
    except:
        if choices.startswith('job:'):
            pass
        elif choices in ('q','quit'):
            print 'Thanks for use, Bye!'
            sys.exit(0)
            return 'quit',choices
        return 'not_in_list',choices
        
        
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
    print select_files('..\\sh\\.','Select shell file','sh')
    print get_user_select_job_list('test_filter_select',['a','b','c'],'sh')
    