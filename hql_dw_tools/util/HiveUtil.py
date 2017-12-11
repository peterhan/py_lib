#!/usr/bin/env python
#!coding:utf8
import subprocess
import os
import sys
import shlex
import logging
import re


class HiveUtil:

    def __init__(self,env=None):
        self.SPLIT_STR = '### stump fo split ###'
        self.env=env

    def hive_call(self, sqls, fname=None, database=None, error_pipe=None):
        '''
        call hive function
        args:
            sql: a long sql statement include all session statement
            fname: if is not None redirect out to fname
        outputs:
            a tuple(out,err) 
            out: full output from hive
            err: standerror output from hive
        '''
        hivecmd = '/data/sysdir/hive/bin/hive'
        #hivecmd = '/data/sysdir/hive-1.0.0/bin/hive'
        FNULL = open(os.devnull, 'w')
        # print FNULL
        #prepare_sql = ' set mapreduce.job.queuename=root.q_dtb.q_dw.q_dw_qa;\n'
        prepare_sql = ' \n'
        if database:
            prepare_sql += ' use %s;'%database
        sqls = prepare_sql + sqls
        cmd = '''
        {0} -e "{1}"'''.format(hivecmd, sqls )
        if error_pipe == 'pipe':
            error_pipe = subprocess.PIPE
        if fname is not None:
            cmd = '''\
            {0} -e "{1}" | gzip > "{2}"'''.format(hivecmd, sqls, fname)
            logging.debug(cmd)
            sp = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE,env=self.env, stderr=error_pipe)
        else:
            args = shlex.split(cmd)
            logging.debug(args)
            sp = subprocess.Popen(
                args, stdout=subprocess.PIPE, env=self.env,stderr=error_pipe)
        out, err = sp.communicate()
        return out, err

    def check_case_syntax_valid(self, case_hql, alias, tbname,):
        sql = "explain select {0} as {1} from {2} where dt='';".format(
            case_hql, alias, tbname)
        out, err = self.hive_call(sql)
        print out, err

    def check_tbl_dt(self, tblname, dt):
        sql = "show partitions {0} partition(dt='{1}');".format(tblname, dt)
        logging.debug(sql)
        out, err = self.hive_call(sql, error_pipe='pipe')
        par = out.splitlines()
        logging.debug(par)
        return len(par), par, out, err

    def add_prefix(self, tblname):
        '''
        auto add database prefix for hive table
        tblname: ads_1111_ec_page_flw
        return: ads.ads_1111_ec_page_flw
        '''
        if tblname.find('.') != -1:
            return tblname
        cpre = tblname.lower().split('_')[0]
        for layer in ['stage', 'ods', 'mds', 'ads', 'rds']:
            if cpre in (layer, layer[0]):
                cpre = layer
                return cpre + '.' + tblname
        return tblname

    def remove_prefix(self, tblname):
        '''
        auto add database prefix for hive table
        tblname: ads.ads_1111_ec_page_flw
        return: ads_1111_ec_page_flw
        '''
        if tblname.find('.') == -1:
            return tblname
        return tblname.lower().split('.')[-1]

    def parse_create_results(self, outs):
        '''
        outs: a multi statement result of "desc table" 
        split by  "select 'SPLIT_STR' ;" result
        return a three dimesional list
        contain the parse result of a hive output
        '''
        rows = []
        pat = re.compile(self.SPLIT_STR)
        r_outs = pat.split(outs)
        for out in r_outs:
            # parse one statement result
            cout, pout, infos = self.parse_single_create_result(out.strip())
            #print cout
            if len(pout) > 0:
                rows.append({'column_info': cout, 'partition_info': pout,'infos':infos})
            else:
                rows.append({'column_info': cout, 'partition_info': None,'infos':infos})
                print 'Not found partition:',out
        return rows
    
    def parse_single_create_result(self,sqlout):
        '''解析单个show create 结果
        '''
        head_pat = re.compile('^[A-Z \(]+$')
        create_pat = re.compile('^CREATE .*$')
        col_pat = re.compile('^`([^`]*)` ([^, ]*) ?(COMMENT)? ?(\'[^\']*\')?,?$')
        par_pat = re.compile('^`([^`]*)` ([^, ]*) ?(COMMENT)? ?(\'[^\']*\')?,?$')
        r_column_rows,partition_rows=[],[]
        info_dic={'sqlout':sqlout}
        capture_flag=False
        current_type='undefine'
        for l in sqlout.splitlines():
            if head_pat.match(l):
                current_type=l.strip('\n( ')
                #print 'define','[%s]'%current_type
            elif create_pat.match(l):
                current_type='CREATE'
                #print 'define','[%s]'%current_type
                info_dic['create_define'] = l.strip('\n( ')
            else:
                current_line=l.strip('\n) ')
                info_dic.setdefault(current_type,[])
                info_dic[current_type].append(current_line)
                #print 'content "%s"'%current_line
        for l in info_dic.get('CREATE',[]):
            m = col_pat.match(l)
            if m is None:
                print 'COL_PARSE_ERR:',l
            else:
                groups = m.groups()
                comment = groups[3].strip("'") if groups[3] else ''
                info = {'col_name':groups[0],'type':groups[1],'comment':comment}
                r_column_rows.append(info)
                #print groups,l
        for l in info_dic.get('PARTITIONED BY',[]):
            m = col_pat.match(l)
            if m is None:
                print 'no partition:',m,l
            else:
                groups= m.groups()
                comment = groups[3] if groups[3] else ''
                info = {'col_name':groups[0],'type':groups[1],'comment':comment}
                partition_rows.append(info)
                #print groups,l
        return r_column_rows,partition_rows,info_dic
    
    def parse_desc_results(self, outs):
        '''
        outs: a multi statement result of "desc table" 
        split by  "select 'SPLIT_STR' ;" result
        return a three dimesional list
        contain the parse result of a hive output
        '''
        rows = []
        pat = re.compile(self.SPLIT_STR)
        r_outs = pat.split(outs)
        for out in r_outs:
            # parse one statement result
            cout, pout = self.parse_single_desc_result(out.strip())
            print cout
            if len(pout) > 0:
                rows.append({'column_info': cout, 'partition_info': pout})
            else:
                print 'Not found partition:',out
        return rows

    def parse_single_desc_result(self, out):
        '''
        out:one describe statement result
        parse one statment return two two dimention array
        first is column info
        [
          [column_name,column_type,column_comment],
          ...
        ]
        second same structure for partition info
        '''
        column_head = ['col_name', 'type', 'comment']
        column_rows = []
        partition_rows = []
        partition_cols = set()
        i_strip = lambda s: s.strip()
        partition_flag = False
        for line in out.splitlines():
            parsed_lst = map(i_strip, line.strip('\n').split('\t', 2))
            if parsed_lst[0] == '':
                continue
            elif line.startswith('#'):
                if line.startswith('# Partition Information'):
                    partition_flag = True
                continue
            elif partition_flag:
                partition_rows.append(dict(zip(column_head, parsed_lst)))
                partition_cols.add(parsed_lst[0])
            else:
                column_rows.append(dict(zip(column_head, parsed_lst)))
        r_column_rows = []
        for row in column_rows:
            if row['col_name'] not in partition_cols:
                r_column_rows.append(row)
        return r_column_rows, partition_rows

    def get_tables_define(self, tblnames, mode='desc'):
        '''
        input:list of table name
        output: three dimension list of  table's parsed describe result
        '''
        sqls = ''
        for tblname in tblnames:
            if mode=='desc':
                sql = ''' desc {0}; '''.format(self.add_prefix(tblname))
            else:
                sql = ''' show create table {0}; '''.format(self.add_prefix(tblname))
            sqls = sqls + sql + "\n!echo %s;\n" % self.SPLIT_STR
        print sqls
        logging.debug(sqls)
        out, err = self.hive_call(sqls)
        #print out
        if mode=='desc':
            tbl_infos = dict(zip(tblnames, self.parse_desc_results(out)))
        else:
            tbl_infos = dict(zip(tblnames, self.parse_create_results(out)))
        return tbl_infos

    def dump_query_result_to_gz_file(self, sql, fname):
        ''' input:sql query
              fname output filename not include ".gz"
        dump a query result to gz file for dimension check
        '''
        logging.debug(sql)
        out, err = self.hive_call(sql, fname)


def test_parse(hu):
    out = '''
subdevice_id        	string              	辅助设备id（android文件的方式） 
sdk_version         	string              	sdk版本号                       
dt                  	string              	分区字段日期              
hour                	string              	分区字段小时              
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
dt                  	string              	分区字段日期              
hour                	string              	分区字段小时              
'''
    print hu.parse_single_desc_result(out)


def test_dt(hu, dt):
    from app_conf import BASEDIR
    tbls = open(BASEDIR + '/data/table_list').read().splitlines()
    print dt
    print tbls
    for tbl in tbls:
        infos = hu.check_tbl_dt(hu.add_prefix(tbl), dt)
        print infos[0], tbl


def test_dump(hu):
    hu.dump_query_result_to_gz_file(
        'select city_id,province_id from dim.dim_city', 'city.gz')
    hu.dump_query_result_to_gz_file(
        'select province_id from dim.dim_province', 'province.gz')

if __name__ == '__main__':
    hu = HiveUtil()
    from qa_util import get_date
    from optparse import OptionParser
    parser = OptionParser()
    dft_date = get_date(days=-1)
    parser.add_option("-d", "--dt", dest="dt",
                      default=dft_date, help='date to import')
    (opts, args) = parser.parse_args()
    test_dt(hu, opts.dt)
    # test_parse(hu)
    # print hu.check_tbl_dt('ods.o_p04_app_event_log_i','2016-12-12')
    # test_dump(hu)
