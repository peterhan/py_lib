from pprint import pprint
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
    if tblname.startswith('tmp_'):
        n_tblname= 'dw_tmp.'+tblname
        _TAP_CACHE[tblname]=n_tblname
        return n_tblname
    _TAP_CACHE[tblname]=tblname
    return tblname

def filter_edge(src,tgt,midx):
    if src.strip()=='' or tgt.strip()=='':
        return True
    if src.find('base_t')!=-1:
        return True
    if src.find('dw_tmp.')!=-1 or src.startswith('tmp_'):
        midx['s_skip'].setdefault(src,set())
        midx['s_skip'][src].add(tgt)
        return True
    if tgt.find('dw_tmp.')!=-1 or tgt.startswith('tmp_'):
        midx['t_skip'].setdefault(tgt,set())
        midx['t_skip'][tgt].add(src)
        return True
    return False

def get_min_len_midx(midx):
    sl=len(midx['s_skip'])
    tl=len(midx['t_skip'])
    print '[Midx] source_skip:',sl,'target_skip:',tl
    return min([sl,tl])

def merge_midx(all_nodes,fedges,bedges,midx):
    n_midx={'s_skip':{},'t_skip':{}}
    s_cnt=0
    t_cnt=0
    tm_cnt=0
    for src,tgts in midx['s_skip'].items():
        if src not in midx['t_skip']:
            tm_cnt+=1
            continue
        srcs=midx['t_skip'][src]
        for src in srcs:
            for tgt in tgts:
                is_f=filter_edge(src,tgt,n_midx)
                if is_f:
                    continue
                fedges.setdefault(src,set())
                bedges.setdefault(tgt,set())
                if tgt not in fedges[src]:
                    t_cnt+=1
                if src not in bedges[tgt]:
                    s_cnt+=1
                all_nodes.add(src)
                all_nodes.add(tgt)
                fedges[src].add(tgt)
                bedges[tgt].add(src)                
    print('[Merge_midx add edges]:src:',s_cnt,'tgt:',t_cnt)
    print('[Terminal TempTable]:',tm_cnt)
    get_min_len_midx(n_midx)
    return n_midx

def load_hql_meta(fname):
    all_nodes=set()
    fedges={}#forward
    bedges={}#backward   
    midx={'s_skip':{},'t_skip':{}}
    for line in open(fname):
        row=line.strip().split('\t')        
        if len(row)<3:continue        
        srcs=row[1].strip('" ').split(',')
        tgts=row[2].strip('" ').split(',')
        for src in srcs:
            src=tblname_add_prefix(src)
            for tgt in tgts:
                tgt=tblname_add_prefix(tgt)
                is_filter=filter_edge(src,tgt,midx)
                if is_filter:
                    continue
                fedges.setdefault(src,set())
                bedges.setdefault(tgt,set())
                fedges[src].add(tgt)
                bedges[tgt].add(src)                
                all_nodes.add(src)
                all_nodes.add(tgt)
    print('[Merge Index]')
    res=get_min_len_midx(midx)
    i=0
    while res>0 and i<10 :
        #raw_input('[Loop]@',i)
        i+=1
        midx=merge_midx(all_nodes,fedges,bedges,midx)
        res=get_min_len_midx(midx)
    return all_nodes,fedges,bedges

if __name__=='__main__':
    res=load_hql_meta("meta_test.xls")
    pprint(res)
