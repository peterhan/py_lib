# import pygraphviz as pgv
# import networkx as nx
# import webbrowser
import optparse 
import collections
import pdb
from load_hql_meta import load_hql_meta
debug=False
FULLNAME=False
def tblname_add_prefix_azk( tblname):
    '''
    auto add database prefix for hive table
    tblname: ads_1111_ec_page_flw
    return: ads.ads_1111_ec_page_flw
    '''
    if tblname.find('.')!=-1:
        #return tblname
        tblname = tblname.split('.')[-1]
    cpre=tblname.lower().split('_')[0]
    for layer in ['stage','ods','mds','ads','rds']:
        if cpre in (layer,layer[0]):
            cpre=layer
            return cpre+'.'+tblname
    return tblname
    
def parse_match(match_term):
    match_rules={}
    pairs=match_term.split(',')
    for pair in pairs:
        key,value=pair.split(':',1)
        if value=='':
            value='should_not_match_any_thing_a_dummy_string'
        match_rules[key]=value
    return match_rules

def process_azk_text_data(fname):
    all_nodes=set()
    fedges=collections.defaultdict(list)  #forward
    bedges=collections.defaultdict(list)  #backward
    for line in open(fname):
        row=line.strip().split('\t')        
        if len(row)<3:
            print '[ParseERROR],',fname,line
            continue        
        tgt=[ 'azk_node.'+s for s in row[1].strip('" ').split(',')]
        src=[tblname_add_prefix_azk(s) for s in row[2].strip('" ').split(',')]
        for s in src:
            #print 'debug',s
            if s.find('dw_tmp.')!=-1:continue
            if s.find('base_t')!=-1:continue
            all_nodes.add(s)
            for t in tgt:
                if t.find('dw_tmp.')!=-1:continue
                fedges[s].append(t)
                bedges[t].append(s)                
                all_nodes.add(t)
    #pdb.set_trace()
    return all_nodes,fedges,bedges

    
def query_nodes(fedges,bedges,match_rules):
    #filter query content
    nodes=set()
    edges=set()
    match_rules=parse_match(match_rules)
    nodes=set(match_rules['node'].split('|'))    
    
    # nodes=all_nodes    
    for i in range(11):
        tnodes,tedges=get_source(nodes,match_rules['direct'],fedges,bedges)        
        nodes.update(tnodes)
        edges.update(tedges)
    if debug:
        print 'edges:',edges
        print 'nodes:',nodes
    return nodes,edges
    
def get_source(nodes,direction,fedges,bedges):
    tnodes=set()
    tedges=set()
    if direction=='down':
        edges=fedges
    else:
        edges=bedges
    for node in nodes:
        tnodes.add(node)
        for tnode in edges.get(node,[]):            
            tnodes.add(tnode)
            if direction=='down':
                tedges.add('%s-%s'%(node,tnode))
            else:
                tedges.add('%s-%s'%(tnode,node))
    return tnodes,tedges

def draw_a_node_set(all_nodes,fedges,bedges):
    for k,v in hanshu.items():
        match_rules='node:%s,direct:%s'%('|'.join(v),'up')
        print match_rules
        nodes,edges=query_nodes(fedges,bedges,match_rules)
        draw_graph(nodes,edges,'ads_1111_%s'%k )
    
def draw_up_down(all_nodes,fedges,bedges):    
    # return
    for direct in ('up','down'):
        for node_target in all_nodes:
            match_rules='node:%s,direct:%s'%(node_target,direct)
            nodes,edges=query_nodes(fedges,bedges,match_rules)
            if len(nodes)==1:
                continue
            draw_graph(nodes,edges,'%s_svg/%s'%(direct,node_target) )
    
def many_starts(st,starts):
    for start in starts:
        if st.startswith(start):
            return True
    return False
    
def many_ends(st,ends):
    for end in ends:
        if st.endswith(end):
            return True            
    return False

def no_pref(st):
    return st.split('.')[-1]    

def query_up(all_nodes,fedges,bedges,start_nodes,end_nodes):
    # return    
    res={}
    for node_target in all_nodes:
        match_rules='node:%s,direct:%s'%(node_target,'up')
        nodes,edges=query_nodes(fedges,bedges,match_rules)
        if len(nodes)==1:
            continue
        if many_starts(node_target,start_nodes) :
            try:
                if FULLNAME:print node_target+'\t'+','.join([node for node in nodes if many_starts(node,end_nodes)])
                else:print no_pref(node_target)+'\t'+','.join([no_pref(node) for node in nodes if many_starts(node,end_nodes)])
                res[node_target]=[node for node in nodes if many_starts(node,end_nodes)]
            except:
                pass
    return res

def query_down(all_nodes,fedges,bedges,start_nodes,end_nodes):
    # return    
    res={}
    for node_target in all_nodes:
        match_rules='node:%s,direct:%s'%(node_target,'down')
        nodes,edges=query_nodes(fedges,bedges,match_rules)
        if len(nodes)==1:
            continue
        if many_starts(node_target,start_nodes) :
            try:
                if FULLNAME:print node_target+'\t'+','.join([node for node in nodes if many_starts(node,end_nodes)])
                else:print no_pref(node_target)+'\t'+','.join([no_pref(node) for node in nodes if many_starts(node,end_nodes)])
                res[node_target]=[node for node in nodes if many_starts(node,end_nodes)]
            except:
                pass
    return res
        
# print nodes
def main():
    global FULLNAME
    optp=optparse.OptionParser()
    optp.add_option('-t',dest='target',default='stage')
    optp.add_option('-s',dest='src',default='mds')
    optp.add_option('-d',dest='down',action='store_true',default=False)
    optp.add_option('-f',dest='full',action='store_true',default=False)
    opts,args=optp.parse_args()
    print opts
    if opts.full:
        FULLNAME=True 
    all_nodes=set()
    fedges=collections.defaultdict(list)  #forward
    bedges=collections.defaultdict(list)  #backward
    for fname in ("meta_data/job_meta_ah_edw_report.xls","meta_data/job_meta_report.xls"):    
        nd,fe,be=load_hql_meta(fname)    
        all_nodes.update(nd)
        fedges.update(fe)
        bedges.update(be)
    '''for fname in ["meta_data/azk_dep_info.xls"]:    
        nd,fe,be=load_hql_meta(fname)    
        all_nodes.update(nd)
        fedges.update(fe)
        bedges.update(be)
    '''
    if opts.down:
        query_down(all_nodes,fedges,bedges,[opts.src+'.'],[opts.target+'.']) 
    else:
        query_up(all_nodes,fedges,bedges,[opts.src+'.'],[opts.target+'.']) 
    
            
if __name__=='__main__':            
    main()
