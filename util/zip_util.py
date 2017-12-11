import  zipfile
import shutil
import datetime

class ContentOperator:
    @staticmethod  
    def test_mod_func(ctnt):        
        nctnt=''        
        for l in ctnt.splitlines():      
            nctnt+=l.replace(' ','  ')+'\n'
        return nctnt
    
def get_ts():
    return datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
def update_file(filename,mod_func):
    #read
    with open(filename)  as  fh:
        ctnt=fh.read()        
    #change
    new_ctnt=mod_func(ctnt)        
    #write
    with open(filename,'w')  as  fh:
        fh.write(new_ctnt)
    return ctnt,new_ctnt

def ls_zipfile(zip_filename):
    rows=[]
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            row=[zinfo.filename,'%04d-%02d-%02d %02d:%02d:%02d'%zinfo.date_time,'%s'%zinfo.file_size,'%s'%zinfo.CRC]
            rows.append(row) 
            print '\t'.join(row)
    print 'file count: ',len(rows)
    return rows
            
def delete_in_zipfile(zip_filename,target_files,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost
    ct_dc={}
    if not not_backup:
        shutil.copy(zip_filename,zip_filename+get_ts()+'.zip')
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            ct_dc[zinfo.filename]=zf_rd.open(zinfo).read()
    #change content of target files
    new_dc={}
    for filename,content in ct_dc.items():
        if filename in target_files:
            print 'Delete:',filename
            continue
        new_dc[filename]=content
    #write back
    with zipfile.ZipFile(zip_filename,'w') as zf_wrt:
        for filename,content in new_dc.items():
            zf_wrt.writestr(filename,content)

       
            
def update_in_zipfile(zip_filename,target_files,mod_funcs,not_backup=False):
    #read zip content into a dict,name duplicate will cause data lost
    ct_dc={}
    if not not_backup:
        shutil.copy(zip_filename,zip_filename+get_ts()+'.zip')
    with zipfile.ZipFile(zip_filename) as zf_rd:
        for zinfo in zf_rd.infolist():
            ct_dc[zinfo.filename]=zf_rd.open(zinfo).read()
    #change content of target files
    for filename,content in ct_dc.items():
        if filename in target_files:
            new_ctnt=content
            for mod_func in mod_funcs:
                new_ctnt=mod_func(new_ctnt)
            ct_dc[filename]=new_ctnt
    #write back
    with zipfile.ZipFile(zip_filename,'w') as zf_wrt:
        for filename,content in ct_dc.items():
            zf_wrt.writestr(filename,content)

def test():
    ls_zipfile('ods_media.zip')
    arr='o_l01_auto_topic_a,o_l03_qareply_a,o_l01_club_owner_camp_a,o_l03_clubcarownercamp_new_a'.split(',')
    arr=map(lambda x:'ods_media/'+x+'.job',arr)
    print arr
    delete_in_zipfile('ods_media.zip',arr)
    
if __name__=='__main__':
    test()
    