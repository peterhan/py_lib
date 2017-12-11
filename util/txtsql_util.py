#!coding:utf8
import os
import re
import glob
import sqlite3
from pprint import pprint
import time,datetime

def create_db(db_file=':memory:'):
    '''初始化数据数据库 返回连接'''
    conn = sqlite3.connect(db_file)
    conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
    return conn

def create_tbl(conn,confs, tb=None,drop=False):
    '''初始化数据表 返回连接'''
    # conn=sqlite3.connect(':memory:')
    tbs = {}
    for conf in confs:
        if len(conf)>=3:
            tbs[conf[0]] = conf[2]
    if tb not in tbs or tb is None:
        return 
    tbname = tb
    colinfo = tbs[tb]
    if drop:conn.execute(''' drop table if exists %s''' % tb )
    sql=''' create table if not exists %s ( %s );''' % (tbname, colinfo)
    print sql
    conn.execute(sql)
    
def read_datename_tsv(dt, tbname, filename, parse_func=None,encode='gbk',filter_func=None):
    '''日期名字结果文件读取'''
    rows = []
    fhandler=open(filename)
    if parse_func is None:
        for l in fhandler:
            l=l.strip('\n')
            if encode=='gbk':
                l=l.decode('gbk').encode('utf8')            
            row = l.split('\t')
            row.insert(0, dt)
            if filter_func is None:
                rows.append(row)
            elif filter_func(row):
                rows.append(row)
            else:
                #print 'skip: ',row
                pass
    else:
        rows=parse_func(dt,fhandler)
    return rows

def insert_table(conn, tbname,rows):
    '''rows行数组灌表'''
    #print 'ready date: %s %s'%( tbname, dts)
    if len(rows) > 0:
        rlen = len(rows[0])
    else:
        print 'Empty insert dataset %s' % tbname
        return
    mask = "insert into %s values(%s)" % (tbname, '?,' * (rlen - 1) + '?')
    print mask, rows[0]
    conn.executemany(mask, rows)
    conn.commit()

def query_db(conn, sql):
    cur = conn.execute(sql)
    for row in cur:
        yield row

def query_db_rows(conn, sql):
    rows=[]
    for row in  query_db(conn, sql):
        rows.append( row)
    return rows

def exec_db(conn, sql):
    cur = conn.execute(sql)
    conn.commit()

def exec_db_many(conn, sqls):
    for sql in sqls:
        exec_db(conn, sql)

def count_date(conn, tb):
    for row in query_db(conn, ''' select '%s',date,count(*) from %s group by date''' % (tb, tb) ):
        print row

def print_query(conn,sql):
    for row in query_db(conn,sql):
        for f in row:
            if type(f)==float:
                st= '%0.6f\t'%f
            else:
                st= '%s\t'%f
            print st.encode('utf8'),
        print ''

def ut_a():
    print load_data_file('20150817', 'mod_matrix', base+'/mod_matrix')
    
def test():
    conn=init_db('query.db')
    # confs=[['query','','date text,type text,query text,pv int']]
    # create_tb(conn,confs,'query',True)
    # rows=read_data_file('20160327', 'mod_matrix','20160327.xls')    
    # load_table(conn,'query',rows,)
    sql='''
    select * from query 
    where query like '%洪%'
    order by 4 desc limit 100
    '''
    for l in  query_db(conn,sql):        
        print '\t'.join(l[:3]).encode('gbk')+'\t%s'%l[3]

if __name__ == '__main__':
    test()
