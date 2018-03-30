#!coding:utf8
from pprint import pprint

# 加表前缀函数缓存字典
_TAP_CACHE={}
def tblname_add_prefix( tblname):
    '''
    auto add database prefix for hive table
    tblname: ads_1111_ec_page_flw
    return: ads.ads_1111_ec_page_flw
    '''
    if tblname.find('.')!=-1:
        return tblname
    if tblname in _TAP_CACHE:
        return _TAP_CACHE[tblname]
    cpre=tblname.lower().split('_')[0]
    for layer in ['stage','dim','ods','mds','ads','rds']:
        if cpre in (layer,layer[0]):
            cpre=layer
            n_tblname= cpre+'.'+tblname
            _TAP_CACHE[tblname]=n_tblname
            return n_tblname
    _TAP_CACHE[tblname]=tblname
    return tblname

def is_skip_edge(src,tgt,reconnect_edge):
    '''should the edge be skip,
        and record parent and child nodes of the node
    '''
    is_skip=False
    is_reonnect=False
    ## 直接跳过节点
    if src.strip()=='' or tgt.strip()=='':        
        is_skip,stype= True,'empty_node_name'
    if src.find('base_t')==0:
        is_skip,stype= True,'with_alias'
    ## 跳过，并记录上下游的节点
    if src.find('dw_tmp.')!=-1 or src.startswith('tmp_'):
        reconnect_edge['up_edge'].setdefault(src,set())
        reconnect_edge['up_edge'][src].add(tgt)
        is_reonnect=True
        is_skip,stype= True,'parent_is_temp'
    if tgt.find('dw_tmp.')!=-1 or tgt.startswith('tmp_'):
        reconnect_edge['down_edge'].setdefault(tgt,set())
        reconnect_edge['down_edge'][tgt].add(src)
        is_reonnect=True
        is_skip,stype= True,'child_is_temp'
    if is_skip:
        action='Skip'
        if is_reonnect:
            action='Reconnect'
        print '[%s Edge (%s)] %s->%s'%(action,stype,src,tgt)
    return is_skip

def count_reconnect_edge(reconnect_edge):
    '''
    '''
    sl=len(reconnect_edge['up_edge'])
    tl=len(reconnect_edge['down_edge'])
    # print '[Count reconnect_edge  length] up_edge:',sl,'down_edge:',tl    
    return min([sl,tl])

def reconnect_nodes(all_nodes,fedges,bedges,reconnect_edge):
    n_reconnect_edge={'up_edge':{},'down_edge':{}}
    s_cnt=0
    t_cnt=0
    tm_cnt=0
    for src,tgts in reconnect_edge['up_edge'].items():
        if src not in reconnect_edge['down_edge']:
            tm_cnt+=1
            continue
        srcs=reconnect_edge['down_edge'][src]
        for src in srcs:
            for tgt in tgts:
                is_skip=is_skip_edge(src,tgt,n_reconnect_edge)
                if is_skip:
                    continue
                fedges.setdefault(src,set())
                bedges.setdefault(tgt,set())
                if tgt not in fedges[src]:
                    t_cnt+=1
                if src not in bedges[tgt]:
                    s_cnt+=1
                all_nodes.update([src,tgt])                
                fedges[src].add(tgt)
                bedges[tgt].add(src)                
    print '[Reconnect_edge add edges]:src:',s_cnt,'tgt:',t_cnt
    print '[End at TempTable]:%s'%tm_cnt
    return n_reconnect_edge

def parse_nodes(st):
    nodes=st.strip('" ').split(',')
    nodes=map(tblname_add_prefix,nodes)
    return nodes
    
def load_hql_meta(fname):
    all_nodes=set()
    fedges={}#forward
    bedges={}#backward   
    # merged index
    reconnect_edge={'up_edge':{},'down_edge':{}}
    for line in open(fname):
        row=line.strip().split('\t')        
        if len(row)<3:
            # wrong format data
            continue        
        srcs=parse_nodes(row[1])
        tgts=parse_nodes(row[2])        
        for src in srcs:            
            for tgt in tgts:                
                is_skip=is_skip_edge(src,tgt,reconnect_edge)
                if is_skip:
                    continue
                fedges.setdefault(src,set())
                bedges.setdefault(tgt,set())
                fedges[src].add(tgt)
                bedges[tgt].add(src)                
                all_nodes.update([src,tgt])  
    print '[Initial reconnect edges]'
    pprint(reconnect_edge)
    reconnect_count=count_reconnect_edge(reconnect_edge)
    i=0
    while reconnect_count>0 and i<10 :
        #raw_input('[Loop]@',i)
        i+=1
        reconnect_edge=reconnect_nodes(all_nodes,fedges,bedges,reconnect_edge)
        reconnect_count=count_reconnect_edge(reconnect_edge)
        print '[Reconnect loop:%s reonnect_count:%s]\n'%(i,reconnect_count)
    return all_nodes,fedges,bedges

if __name__=='__main__':
    res=load_hql_meta("meta_test.xls")
    names='all_node,forward,backward'.split(',')
    res=zip(names,res)
    print ''
    pprint(res)
