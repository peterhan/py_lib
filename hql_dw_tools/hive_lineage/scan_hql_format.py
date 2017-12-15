import sqlparse
import glob
import collections
import logging
import pdb

from sqlparse import tokens as Token

def u2g(st):
    return unicode(st).encode('gbk','ignore')

def get_tok_name(tok):
    nmtok=tok.normalized
    new_st='("%s",%s) '%(nmtok.replace('\n','\\n'),tok.ttype)
    if nmtok.find('\n')!=-1:
        new_st+='\n'
    return unicode(new_st)
    
def get_flat_toks_name(flat_toks):
    return ' ,'.join(map(get_tok_name,flat_toks))

def tblname_add_prefix( tblname):
    '''
    auto add database prefix for hive table
    tblname: ads_1111_ec_page_flw
    return: ads.ads_1111_ec_page_flw
    '''
    if tblname.find('.')!=-1:
        return tblname
    cpre=tblname.lower().split('_')[0]
    for layer in ['stage','ods','mds','ads','rds']:
        if cpre in (layer,layer[0]):
            cpre=layer
            return cpre+'.'+tblname
    return tblname
    
def guess_table_name_from_path(path):
    tbname=path.split('/')[-1]
    tbname=tblname_add_prefix(tbname)
    return tbname
    
def peek_sql(sql):
    stmts=sqlparse.parse(sql)
    stmt=stmts[0]
    print 'Statement type:',stmt.get_type()
    flat_tok=[tok for tok in stmt.flatten()]
    print 'Flatten Tokens:',map(get_tok_name,flat_tok),len(flat_tok)
    print 'Nested Tokens:',map(get_tok_name,stmt.tokens),len(stmt.tokens)
    for tok in stmt.tokens:
        print get_tok_name(tok),
        if tok.is_group:
            print '[sub]:',tok.tokens
    print '#'*8
    return stmt
    
def get_followed_tablename(i,flat_toks):  
    tok=flat_toks[i+1]
    tbname=[tok.value]
    for j in range(3):
        p_idx=i+2+j*2
        n_idx=i+3+j*2
        if n_idx>=len(flat_toks):
            break
        # print 'p,n:',flat_toks[p_idx],flat_toks[n_idx]
        if flat_toks[p_idx].value!='.':
            break
        elif flat_toks[n_idx].ttype==Token.Name:
            tbname.append(flat_toks[n_idx].value)
    return '.'.join(tbname).lower()
    
def parenthetic_contents(string):
    """Generate parenthesized contents in string as pairs (level, contents)."""
    stack = []
    for i, c in enumerate(string):
        if c == '(':
            stack.append(i)
        elif c == ')' and stack:
            start = stack.pop()
            yield (len(stack), string[start + 1: i])
            

        
def parse_hql(hql):
    '''parse a series of hql'''
    file_info={'depend_table':[],'with_alias':[],'operate_table':[],'create_function':[],'add_jar':[]}
    print sqlparse.format(hql,reindent=True,keyword_case='upper',identifier_case='lower',wrap_after=80)
        
        
    


if __name__=='__main__':    
    path='hql_parse_test.hql'         
    forceDebug=False
    # forceDebug=True
    import optparse,sys
    parser = optparse.OptionParser()
    parser.add_option('-p', '--path', action="store", dest="path", help="path", default=path)
    parser.add_option('-d', '--debug', action="store_true", dest="debug", help="debug", default=forceDebug)
    opts, args = parser.parse_args()
    print>>sys.stderr, 'Path mask: ', opts.path
    
    if opts.debug:
        logging.basicConfig(level=logging.DEBUG)
    parse_info = parse_hql(open(opts.path).read())
    

