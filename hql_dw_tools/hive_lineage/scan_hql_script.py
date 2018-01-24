import sqlparse
import glob
import collections
import logging

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
    for statement in sqlparse.parse(hql):
        # print '\n[%s]:'%statement.get_type() 
        # print statement
        flat_toks=[]
        for token in statement.flatten():
            if token.ttype not in (Token.Text.Whitespace, Token.Text.Whitespace.Newline):
                flat_toks.append(token)
        len_flat_toks=len(flat_toks)
        nest_toks=statement.tokens
        stmt_type=statement.get_type()
        
        logging.debug('\n\n\n[Statement Find]') 
        logging.debug('[Statement type]: %s',stmt_type)            
        logging.debug('[Statement content]:\n%s',statement)
        logging.debug('[Token List]:\n%s',get_flat_toks_name(flat_toks).encode('gbk','ignore'))
        logging.debug('\n[Start Parse:]')
        
        for i,tok in enumerate(flat_toks):
            this_token=tok.normalized
            
            ### select from
            if (this_token in ('FROM','JOIN') or this_token.endswith('JOIN')) \
                and tok.ttype==Token.Keyword:
                logging.debug('[FROM/JOIN]Clause Found')
                if len_flat_toks>=(i+1) and flat_toks[i+1].ttype!=Token.Name:                        
                    logging.debug('[Warning:FROM/JOIN@skip1]next_tok.ttype!=Token.Name')
                    continue
                tbname=get_followed_tablename(i,flat_toks)                    
                logging.debug('[FROM/JOIN Found tbname] %s',tbname)
                file_info['depend_table'].append(tbname)
                
            ### WITH clause
            if  this_token=='WITH'\
                and tok.ttype==Token.Keyword.CTE:
                logging.debug('[WITH Clause Find]')
                next_tok = flat_toks[i+1]
                logging.debug('[WITH Clause Find]:%s',next_tok)                    
                alias_name=next_tok.value
                file_info['with_alias'].append(alias_name)
                # find 2nd,3rd.. with clause
                for j in range(i,len_flat_toks):
                    if flat_toks[j].value.upper()=='INSERT':
                        logging.debug( 'with break on insert')
                        break
                    if flat_toks[j].value==')' and flat_toks[j+1].value.upper()=='SELECT':
                        logging.debug('with break on )select')
                        break
                    if flat_toks[j].value==',' and flat_toks[j+2].value.upper()=='AS' and flat_toks[j+3].value=='(':
                        alias =flat_toks[j+1].value
                        logging.debug( '[WITH Cluase Series Table]:%s',alias)
                        file_info['with_alias'].append(alias)
            
            ### insert ,create to                
            if stmt_type in ('UNKNOWN','CREATE','INSERT','CREATE OR REPLACE') \
                and this_token in ('TABLE','VIEW') \
                and tok.ttype==Token.Keyword:
                logging.debug('[Insert/Create]Clause Found')
                if len_flat_toks>i and flat_toks[i+1].ttype!=Token.Name:
                    logging.debug('[Warning:Insert/Create@skip_next_not_token.name]')
                    continue
                if  i>1 and (flat_toks[i-1].value).lower()=='lateral': #lateral view will trigger                   
                    logging.debug('[Warning:Insert/Create@skip_lateral]')
                    continue
                tbname=get_followed_tablename(i,flat_toks)
                logging.debug('[Insert/Create Found tbname] %s',tbname)
                file_info['operate_table'].append(tbname)
                
            ### insert create to directory                
            if stmt_type in ('UNKNOWN','CREATE','INSERT','CREATE OR REPLACE') \
                and this_token in ('directory','DIRECTORY') \
                and tok.ttype==Token.Name:
                logging.debug('[Warning:DIRECTORY Insert Find]')
                if len_flat_toks>i and flat_toks[i+1].ttype!=Token.Literal.String.Single:                        
                    logging.debug( '[Warning:DIRECTORY Insert skip] nextToke!=Token.Literal.String.Single')
                    continue
                dirname = get_followed_tablename(i,flat_toks).strip("'")
                tbname = guess_table_name_from_path(dirname)
                logging.debug('[DIRECTORY Insert Found tbname] %s',tbname)
                file_info['operate_table'].append(tbname)
                
            ### temp function
            if stmt_type=='CREATE' and this_token=='CREATE'\
                and flat_toks[i+1].normalized=='TEMPORARY'  \
                and flat_toks[i+2].normalized=='FUNCTION':
                logging.debug('[FUNCTION Create Find]')
                func_name=flat_toks[i+3].normalized.strip(';')
                class_name=flat_toks[i+5].normalized.strip(';')                    
                logging.debug('[FUNCTION Create]: func: %s class:%s',func_name,class_name)
                file_info['create_function'].append('%s:%s'%(func_name,class_name))
                
            ### add jar
            if stmt_type=='UNKNOWN' and this_token=='ADD'\
                and flat_toks[i+1].normalized=='jar' :
                logging.debug('[ADD Jar Find]')
                nameparts=[tok.value for tok in nest_toks[i+4:-1]]
                file_name=''.join(nameparts).strip()
                logging.debug('[ADD Jar FileName]:%s',file_name)
                file_info['add_jar'].append(file_name)
    return file_info
    
def parse_hql_file_info(path):
    ''' parse the hql file in the path
    '''
    parse_info={}
    for fname in glob.glob(path):
        # print 'Process File:',fname
        hql=''.join([l.replace('\\n','\\\\n') for l in open(fname).readlines()])
        # hql= sqlparse.format( st,reindent=True)
        file_info=parse_hql(hql)
        parse_info[fname]=file_info
        #### print result
        logging.debug('[parse_info]:%s',parse_info)
    return parse_info
        
def print_parse_info(parse_info):
    #### print parse_info    
    for fname,hql_info in parse_info.items():
        deps = list(set(hql_info['depend_table']) - set(hql_info['with_alias']))
        with_c = list(hql_info['with_alias'])
        oper = list(set(hql_info['operate_table']))
        creates = list(set(hql_info['create_function']))
        jars =  list(set(hql_info['add_jar']))
        t=[deps,oper,creates,jars,with_c]
        t=map(lambda x:','.join(x),t)
        t.insert(0,fname)
        print '\t'.join(t)

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
    parse_info = parse_hql_file_info(opts.path)
    ## print 
    print_parse_info(parse_info)

