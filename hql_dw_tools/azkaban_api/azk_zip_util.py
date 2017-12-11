import zipfile
import shutil,os
import datetime


def get_ts():
    return datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
def noop_file_handler(zip_filename,noopname,op_type,op_nodes):
    '''read zip file 's noop file,
    return a edited noop file content
    op_type is add or del
    '''
    content=''
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            # print zinfo.filename
            if zinfo.filename.endswith(noopname):
                print '[NoopHandler read noop.job file from zip]:',zinfo.filename,noopname
                content=zf_rd.open(zinfo).read()
    new_content_lst=[]
    for line in content.splitlines():
        if line.find('dependencies=')!=-1:
            lst_st=line.split('=')[-1]
            deps=set(lst_st.replace(' ','').split(','))
            if op_type=='add':
                deps.update(op_nodes)
            elif op_type=='del' :
                deps = deps - set(op_nodes)
            #### find mode
            elif op_type=='find' :
                find_result = deps.intersection(set(op_nodes))            
                if len(find_result)>0:
                    return find_result
                return []
            else:
                print '[No Option Type]'
                return False
            sorted_deps=sorted(list(deps))
            new_line='dependencies='+','.join(sorted_deps)
            new_content_lst.append(new_line)            
        else:
            new_content_lst.append(line)
    if len(new_content_lst)>0:
        return '\n'.join(new_content_lst)
    else:
        print '[Not exists or empty noop file!]'
        return False
    

def ls_zipfile(zip_filename,is_print=True):
    rows=[]
    maxlen=0
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            nml = len(zinfo.filename)
            if nml>maxlen:
                maxlen=nml
            row=[zinfo.filename,'%04d-%02d-%02d %02d:%02d:%02d'%zinfo.date_time,'%s'%zinfo.file_size,'%s'%zinfo.CRC]
            rows.append(row) 
    rows=sorted(rows)
    if is_print:
        for row in sorted(rows):
            print row[0].ljust(maxlen)+'\t'+'\t'.join(row[1:])
    if is_print:
        print 'file count: ',len(rows)
    return rows


def read_zip_content(zip_filename):
    '''read zipfile into memory dict 
        dict format: filename:[zinfo,content]
    '''
    ct_dc={}
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            ct_dc[zinfo.filename]=[zinfo,zf_rd.open(zinfo).read()]
    return ct_dc
    
def write_zip_content(zip_filename,new_ctnt_dc):
    '''write memory dict into zipfile
        dict format: filename:[zinfo,content]
    '''
    with zipfile.ZipFile(zip_filename,'w') as zf_wrt:
        for filename,info_list in new_ctnt_dc.items():
            zinfo,content = info_list
            zf_wrt.writestr(zinfo,content)
    return True
    
def make_zip_backup(zip_filename):
    folder='.zip_back/'+os.path.split(zip_filename)[0]
    if not os.path.exists(folder):
        os.makedirs(folder)
    bak_fn='.zip_back/'+zip_filename+'.'+get_ts()+'.zip'
    print '[ZipFile backup to]: '+bak_fn
    if not os.path.exists(bak_fn):
        shutil.copy(zip_filename,bak_fn)
    return True    
    
def delete_in_zipfile(zip_filename,target_files,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost    
    if not not_backup:
        make_zip_backup(zip_filename)
    ct_dc = read_zip_content(zip_filename)
    #change content of target files
    new_dc={}
    for filename,obj in ct_dc.items():
        zinfo,content = obj
        if filename in target_files:
            print 'Delete:',filename
            if not not_backup:
                new_dc[filename]=[filename+'.bak',content]
            continue
        new_dc[filename]=obj
    #write back
    write_zip_content(zip_filename,new_dc)    
    return True
            
            
def add_to_zip_file(zip_filename,member_name,ctnt,strict=False):
    ct_dc = read_zip_content(zip_filename)
    if member_name in ct_dc:
        make_zip_backup(zip_filename)
        print '[Warning: ZipFile member OverWrite!]:',member_name
        if strict:
            return False
    ct_dc[member_name] = [member_name,ctnt]
    write_zip_content(zip_filename,ct_dc)
    return True
     
def replace_in_zipfile(zip_filename,target_file,new_ctnt,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost    
    if not not_backup:
        make_zip_backup(zip_filename)
    ct_dc = read_zip_content(zip_filename)
    #change content of target files    
    is_change=False
    for filename,obj in ct_dc.items():
        zinfo,content = obj        
        if filename.endswith(target_file) :            
            ct_dc[filename]=[filename,new_ctnt]
            is_change=True
    if not is_change:
        print '\n[Error:Not Exists Zip member,Fail to update]: %s\n'%target_file
        return False
    #write back
    write_zip_content(zip_filename,ct_dc)
    return True
    
def replace_many_in_zipfile(zip_filename,target_files,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost    
    if not not_backup:
        make_zip_backup(zip_filename)
    ct_dc = read_zip_content(zip_filename)
    #change content of target files
    for filename,obj in ct_dc.items():
        zinfo,content = obj
        for target_file,new_ctnt in target_files.items():
            if filename.endswith(target_file) :            
                ct_dc[filename]=[filename,new_ctnt]
    #write back
    write_zip_content(zip_filename,ct_dc)
    return True
    
def update_in_zipfile(zip_filename,target_files,mod_funcs,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost    
    if not not_backup:
        make_zip_backup(zip_filename)
    ct_dc = read_zip_content(zip_filename)
    #change content of target files
    for filename,obj in ct_dc.items():
        zinfo,content = obj
        for target_file in target_files:
            if filename.endswith(target_file) :
                new_ctnt=content
                for mod_func in mod_funcs:
                    try:
                        new_ctnt=mod_func(new_ctnt)
                    except:
                        print '[ZipFile ChangeFail]',zip_filename,filename,mod_func
                ct_dc[filename]=[filename,new_ctnt]
    #write back
    write_zip_content(zip_filename,ct_dc)
    return True
                   
def test_update_file(filename,mod_func):
    #read
    with open(filename)  as  fh:
        ctnt=fh.read()        
    #change
    new_ctnt=mod_func(ctnt)        
    #write
    with open(filename,'w')  as  fh:
        fh.write(new_ctnt)
    return ctnt,new_ctnt
    
def func_test():
    ls_zipfile('ods_media.zip')
    arr='o_l01_auto_topic_a,o_l03_qareply_a,o_l01_club_owner_camp_a,o_l03_clubcarownercamp_new_a'.split(',')
    arr=map(lambda x:'ods_media/'+x+'.job',arr)
    print arr
    delete_in_zipfile('ods_media.zip',arr)
    
if __name__=='__main__':
    func_test()
    print ''
