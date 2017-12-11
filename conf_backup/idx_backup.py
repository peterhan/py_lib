import os,sys

def isblack(st):
    midblacks = ['nohup.out','/.', '.svn','.git' ]
    endblacks = ['.o','.lo','.pyc','.crc','.back.tar']
    for black in midblacks:
        if st.find(black)!= -1:
            return True
    for black in endblacks:
        if st.endswith(black):
            return True
    return False

def iter_folder(basepath):
    for root, dirs, files in os.walk(basepath):
        if len(files)>1000:
            print '## too many files in : %s ' %root
            continue
        for file in files:
            path = root+'/'+file
            if isblack(path) or isblack(root):
                print '## skip backup file :  %s'%(path)
                continue
            try:size = os.path.getsize(path)
            except:size = 0
            yield path,size
import datetime
def get_date(**kwargs):
    return datetime.datetime.now() + datetime.timedelta( **kwargs)

def backup(user):
    lst=[]
    biglst=[]
    for file,size in iter_folder('/home/work/%s'%user):
        if size < 20480:
            lst.append(file)
        else:
            print '## Big file not backup: %s ,%s'%(file,size)
            biglst.append(file)
    gen_sh(lst, 'small', user)
    gen_sh(biglst, 'big', user)


def gen_sh(lst, type, user):
    step = 1000
    st = ''
    tlst = []
    dt =  get_date().strftime('%Y%m%d') 
    tag = '%s.%s.%s'%(user, type,dt)
    def write_sh(lst  ):
        st = 'tar -uvf %s.backup.tar  '%(tag) + \
         '"%s"\n'%'"  \\\n  "'.join( lst) 
        return st 
    print '### total: %s'% len(lst)
    for i,file in enumerate(lst):
        tlst.append(file)
        if i>0 and i % step ==0:
            tlst.append(lst.pop(0))
            st += write_sh(tlst)
            tlst = []
    st+= write_sh(tlst )
    out=open('file/exe_backup_%s.sh'%(tag),'w')
    out.write(st )
    out.write("\n")
    out.close()
    return st

backuplist=['hanhan', "hanshu","dengwei","wangxuanchun","chengong","fengjie","wangyan35","mahao"]
#backuplist=['hanshu',]
for user in backuplist:
    backup(user)
