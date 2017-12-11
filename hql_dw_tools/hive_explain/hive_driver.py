#!coding:utf8
import os,shlex,subprocess,logging
logging.basicConfig(level=logging.DEBUG)

def hive_call( sqls, fname=None,error_pipe=None):
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
    setqueue = ' set mapreduce.job.queuename=root.q_dtb.q_dw.q_dw_qa;\n'
    sqls = setqueue + sqls
    cmd = '''
    {0} -e "{1}"'''.format(hivecmd, sqls )
    if error_pipe=='pipe':
        error_pipe=subprocess.PIPE
    if fname is not None:
        cmd = '''\
        {0} -e "{1}" | gzip > "{2}"'''.format(hivecmd, sqls, fname)
        logging.debug(cmd)
        sp = subprocess.Popen( cmd,shell=True, stdout=subprocess.PIPE, stderr=error_pipe)
    else:
        args = shlex.split(cmd)
        logging.debug(args)
        sp = subprocess.Popen( args, stdout=subprocess.PIPE, stderr=error_pipe)
    out, err = sp.communicate()
    return out, err

def call_hive():
    sql='''
    set hive.explain.user=true ;
    explain
        select
            a.id                                                                --订单id
         
        ;
    '''
    #out,err=hive_call(sql,fname='a.gz',error_pipe='pipe')
    out,err=hive_call(sql,error_pipe='pipe')
    print out
    print err

call_hive()
