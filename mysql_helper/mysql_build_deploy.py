import glob,sqlparse
import os,re,collections
import sys

def get_whitelist(argv):
    whitelist=[]
    fn='full_white_list.txt' if len(argv)<2 else argv[1]
    print 'read white list from:',fn
    for l in open(fn):
        whitelist.append(l.strip().strip('#'))
    return whitelist
    
cnt={} 
def merge_sql(prefix,type,whitelist):
    global cnt
    obj_lst=[]
    with open('deploy_sql/%s_%s.sql'%(prefix,type),'w') as fh:
        for obj_fn in glob.glob('split_db_obj/%s*.sql'%prefix):
            bfn= os.path.basename(obj_fn)
            info_arr=bfn.split('.')
            oname=info_arr[2]
            type=info_arr[1]
            if oname not in whitelist and not oname.startswith(whitelist[0]) :
                continue
            #print obj_fn
            obj_lst.append(obj_fn.split('.'))
            cnt.setdefault(type,0)
            cnt[type]+=1
            for l in open(obj_fn):
                fh.write(l)
    return obj_lst
                
def main():
    whitelist=get_whitelist(sys.argv)
    objl=[]
    for i,name in enumerate(['table','temp_table','view','func','proc','event']):
        #print i,name
        tmpl=merge_sql(i,name,whitelist)
        objl.extend(tmpl)
    print 'output summary: ', cnt
    objn= [e[2]  for e in objl ]
    print '[Warning] not output objects :',[e for e in whitelist  if e not in objn]
    
if __name__=='__main__':
    main()
