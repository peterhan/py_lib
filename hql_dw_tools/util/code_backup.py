import hashlib
import os,sys
import glob
import json
import socket
import zipfile
import datetime
base='/data/sysdir/xxx'
 
def calc_sha1(filepath):
    with open(filepath,'rb') as f:
        sha1obj = hashlib.sha1()
        sha1obj.update(f.read())
        hash = sha1obj.hexdigest()
        return hash

def scan_folder_finger_print(path):
    dc={}
    fns=[]
    for fname in glob.glob(path):
        fns.append(fname)
        key=fname.replace(base,'')
        dc[key]=calc_sha1(fname)
    return dc,fns
        

def main():
    ## backup the code
    paths=[]
    for lvl in ('/*'*5,'/*/*/*/*','/*/*/*'):
        for suffix in ('.sh','.hql','.py'):#,'.jar'
            path=base+lvl+suffix
            paths.append(path)
    host=socket.gethostname()
    fp_data={}
    all_fn=[]
    for path in paths:
        thedata,fns=scan_folder_finger_print(path)
        fp_data.update(thedata)
        all_fn.extend(fns)
    dt=datetime.datetime.now().strftime('%Y%m%d')
    zfh= zipfile.ZipFile('code_bak/backup.%s.%s.zip'%(host,dt),'w')
    for fn in all_fn:
        zfh.write(fn)
    json.dump(fp_data,open('fp.%s.json'%host,'w'),indent=2)
    #for k,v in data.items(): print '\t'.join([k,'lq2',v])

if __name__=='__main__':
    main()
