import glob
import sqlparse
import os,re,collections

comm_header='''
-- MySQL dump 10.13  Distrib 5.7.12-5, for Linux (x86_64)
-- Host: 192.168.219.62    Database: dw_ec_order_etl
-- Server version	5.7.13
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET TIME_ZONE='+08:00' */;
/*!50106 SET @save_time_zone= 'SYSTEM' */ ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
'''



########## syntax pattern
t_pat=re.compile('-- Table structure for table `(.*)`')
temp_t_pat=re.compile('-- Temporary view structure for view `(.*)`')
v_pat=re.compile('-- Final view structure for view `(.*)`')
f_pat=re.compile('/\*\!\d+ DROP FUNCTION IF EXISTS `(.*)` \*/;')
p_pat=re.compile('/\*\!\d+ DROP PROCEDURE IF EXISTS `(.*)` \*/;')
e_pat=re.compile('/\*\!\d+ DROP EVENT IF EXISTS `(.*)` \*/;')
########## object type
pat_info={'table':t_pat,'temp_table_for_view':temp_t_pat,'view':v_pat,'func':f_pat,'procedure':p_pat,'event':e_pat}
pat_prior={'table':0,'temp_table_for_view':1,'view':2,'func':3,'procedure':4,'event':5}
########## clean pattern
clean_pat=re.compile(r'/\*!\d+(.*)\*/(.*)');
clean_def=re.compile(r'DEFINER=\`ETLWriter\`@\`\d+\.\d+\.\d+\.\d+\`')
# clean_def=re.compile(r'DEFINER')

cnt={}
class DBObj():
    
    def __init__(self,obj_name,type,fname,reformat=False):
        self.obj_name=obj_name
        self.type=type
        self.fname=fname
        self.prior=pat_prior.get(type,'z')
        prefix="-- [dump from "+self.fname+" ]\n"
        self.sql=prefix+comm_header
        if type=='event':
            self.sql=prefix+'DELIMITER ;;\n'+comm_header.replace(';',';;')
        self.reformat=reformat
        
    def append(self,l):
        #clean definer
        md=clean_def.search(l)
        if md:
            # print l
            l=clean_def.sub('DEFINER=`ETLWriter`@`%`',l)
            # print l
        #prettyfi the sql
        # if len(l)>120:l=sqlparse.format(l,reindent=True).encode('utf8')
        # add to script
        if self.reformat==False:
            self.sql+=l
            return
        # clean the /*!500030 */
        m=clean_pat.match(l)
        if m:
            grps=m.groups()
            ln=''.join(grps)+'\n'
            
            self.sql+=ln
            if ln.find('*/')!=-1:print grps,l
            # exit()
        else:
            self.sql+=l
            
    def output(self):
        global cnt
        cnt.setdefault(self.type,0)
        cnt[self.type]+=1        
        fname='split_db_obj/{0}.{1}.{2}.sql'.format(self.prior,self.type,self.obj_name)
        if os.path.exists(fname):
            # print 'file exists',fname 
            # return
            # print 'file overwrite',fname 
            pass
        with open(fname,'w') as fh:
            fh.write(self.sql)


def proc_dump(fname):
    obj=DBObj('all','header',fname)
    for l in open(fname).readlines():
        for k,v in pat_info.items():
            m=v.match(l)
            if m:
                obj_name= m.group(1) 
                type=k
                # print 'Find DBObject:',obj_name,type
                obj.output()
                obj=DBObj(obj_name,type,fname)
                if type=='2.view':
                    obj.append('DROP TABLE IF EXISTS %s ;-- add by split py\n'%obj_name)
                break    
        obj.append(l)
    obj.output()
        
def main():
    for fdname in ('split_db_obj','deploy_sql','dump_sql'):
        if not os.path.exists(fdname):
            print 'mkdir:',fdname
            os.mkdir(fdname)    
    flist=[fn for fn in glob.glob('dump_sql/*.sql')]
    # proc_dump(flist[1])
    for fname in flist: 
        print 'proc file: ',fname
        proc_dump(fname)
    print 'proc obj cnt:'
    for k,v in cnt.items():
        print k,v,';',
    
if __name__=='__main__':
    main()
