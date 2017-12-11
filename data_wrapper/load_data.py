#!coding:utf8
import os
import re
import glob
import sqlite3
from pprint import pprint
import time
import datetime

FMT = '%Y%m%d'


def get_date(fmt=FMT, base=datetime.datetime.now(), isobj=False, **kwargs):
    '''
    日期计算
    '''
    dateobj = base + datetime.timedelta(**kwargs)
    if isobj:
        return dateobj
    else:
        return dateobj.strftime(fmt)


def str2date(str_date, fmt=FMT):
    '''
    字符串转日期
    '''
    dobj = datetime.datetime.fromtimestamp(
        time.mktime(time.strptime(str_date, fmt)))
    return dobj


def get_ready_dates(con, tbname):
    '''
    找出已有数据日期
    '''
    sql = 'select distinct date from %s' % tbname
    dts = []
    for dt in con.execute(sql):
        dts.append(dt[0])
    return dts


def load_mod_matrix(tbname, fn):
    '''读取展开mod_matrix文件'''
    rows = []
    today = get_date()
    for l in open(fn):
        if l.startswith('#'):
            continue
        record = l.strip().decode('gbk').encode('utf8').split('\t')
        start, end = record[0].split(',')
        dts = []
        for i in range(100):
            dt = get_date(base=str2date(start), days=i)
            dts.append(dt)
            if dt >= end or dt >= today:
                break
        mod = dict(
            zip(['baidu', 'sogou', 'sm', 'digit', 'google'], record[1:]))
        for dt in dts:
            for k in mod:
                row = [dt, k, mod[k]]
                rows.append(row)
    return rows


def load_data_file(dt, tbname, fn):
    '''结果文件读取'''
    rows = []
    fh = open(fn)
    fieldcnt = None
    if tbname == 'mod_matrix':
        return load_mod_matrix(tbname, fn)
    for l in fh:
        if tbname == 'refernum':
            row = l.strip().decode('gbk').encode('utf8').split('\t')
            row[0] = dt
            keys = row[1].split('_')
            row[1] = keys[0]
            for i in range(1, 3):
                if len(keys) > i:
                    row.insert(1 + i, keys[i])
                else:
                    row.insert(1 + i, '')
        elif tbname == 'wise_dim':
            row = l.strip().split('\t')
            row.insert(0, dt)
            keys = row[1].split('|')
            row[1] = keys[0]
            row.insert(2, keys[1])
        elif tbname == 'wise_share':
            row = l.strip().decode('gbk').encode('utf8').split('\t')
            row.insert(0, dt)
            row[1] = row[1].replace('share_', '')
        else:
            row = l.strip().decode('gbk').encode('utf8').split('\t')
            row.insert(0, dt)
        if not fieldcnt:
            fieldcnt = len(row)
        if len(row) == fieldcnt:
            rows.append(row)
        else:
            print 'short row', row
    return rows


def load_table(conn, tbname, file_loc):
    '''批量结果文件灌表'''
    PAT_DATE = re.compile('\D(\d{8})\D?')
    rows = []
    dts = get_ready_dates(conn, tbname)
    print 'Table %s last 10 before loaded date' % tbname
    pprint(dts[-10:])
    # print 'ready date: %s %s'%( tbname, dts)
    for fn in glob.glob(file_loc):
        sr = PAT_DATE.search(fn)
        if sr is not None:
            dt = sr.group(1)
        else:
            print sr, fn
            dt = ''
        if dt in dts:
            continue
        e_rows = load_data_file(dt, tbname, fn)
        print dt, tbname, fn, len(e_rows)
        rows.extend(e_rows)
    if len(rows) > 0:
        rlen = len(rows[0])
    else:
        print 'Empty insert dataset %s' % tbname
        return
    mask = "insert into %s values(%s)" % (tbname, '?,' * (rlen - 1) + '?')
    print mask, rows[0]
    conn.executemany(mask, rows)
    conn.commit()


def init_tb(conf, tb=None, drop=False, db_file=':memory:'):
    '''初始化数据表 返回连接'''
    # conn=sqlite3.connect(':memory:')
    conn = sqlite3.connect(db_file)
    conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
    tbs = {}
    for record in conf:
        tbs[record[0]] = record[2]
    if tb not in tbs or tb is None:
        return conn
    tbname = tb
    colinfo = tbs[tb]
    if drop:
        conn.execute(''' drop table if exists %s''' % tb )
    conn.execute(''' create table if not exists %s ( %s );''' %
                 (tbname, colinfo))
    return conn


def query_db(c, sql):
    cur = c.execute(sql)
    for row in cur:
        yield row


def count_date(c, tb):
    for row in query_db(c, ''' select '%s',date,count(*) from %s group by date''' % (tb, tb) ):
        print row

BASE = "/home/work/dengwei/task/150416_HOLMES_SITE/export"


def main():
    conf = [
        ['refernum',   '../wise_market_share/refernum/all_*.xls',
            'date text, dim text, vlu text, se text, pv int, share float'],
        ['clickprob',  '../wise_market_share/clickprob/20*',
            'date text, se text, clickprob float'],
        ['wise_dim',   '/home/work/hanshu/wise_dimension/data/pvuv/pvuv.20*',
            'date text, dim text, vlu text,  pv int , uv int'],
        ['wise_share', BASE + '/chk/20*.new.check',
            'date text, se text, share float'],
        ['mod_matrix', BASE + '/mod_matrix', 'date text, se text, mod float'],
    ]
    con = init_tb(conf, db_file='wise_share.db')
    for tb, loc, cols in conf[:]:
        init_tb(conf, tb, db_file='wise_share.db')
        if tb in ('mod_matrix'):
            init_tb(conf, tb, drop=True)
        load_table(con, tb, loc)
    #for tb in ['wise_dim','refernum','clickprob']:count_date(c,tb)
    #for tb in ['wise_dim',]: count_date(c,tb)


def test():
    # print load_data_file('20150417', 'wise_dim', '/home/work/hanshu/wise_dimension/data/pvuv/pvuv.20150417')
    # print load_data_file('20150617', 'refernum', '../refernum/all_20150617.xls')
    # print load_data_file('20150817', 'wise_share',
    # base+'/chk/20150817.new.check')
    print load_data_file('20150817', 'mod_matrix', base + '/mod_matrix')

if __name__ == '__main__':
    main()
    # test()
