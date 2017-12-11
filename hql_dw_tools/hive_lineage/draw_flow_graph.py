# import pygraphviz as pgv
# import networkx as nx
import webbrowser
import optparse 
import collections
import sys,os
from load_hql_meta import load_hql_meta
from draw_cytoscape import draw_cytoscape
debug=False
DATA_FILE="meta_data/job_meta_report.xls"

    
def draw_graph(nodes,edges,outfile):
    #circo dot fdp neato nop nop1 nop2 osage patchwork sfdp twopi
    G=pgv.AGraph(strict=False, directed=True,size = "10,10!",ratio="auto",layout="dot")
    # G=nx.Graph()
    g_name=outfile
    sg=G.add_subgraph(name='cluster:%s'%g_name,label=g_name)
    for node in nodes:        
        try:sg.add_node(node)
        except:print 'add node fail',node
    for edge in edges:
        pair=edge.split('-')
        snode=pair[0]
        enode=pair[1]
        try:sg.add_edge(snode,enode)
        except:print 'add edge fail',snode,enode
    # print G
    outfile=outfile+'.svg'
    G.draw(outfile, prog='neato') 
    # webbrowser.open(outfile)
    
def parse_match(match_term):
    match_rules={}
    pairs=match_term.split(',')
    for pair in pairs:
        key,value=pair.split(':',1)
        if value=='':
            value='should_not_match_any_thing_a_dummy_string'
        match_rules[key]=value
    return match_rules

    
def query_nodes(fedges,bedges,match_rules):
    #filter query content
    nodes=set()
    edges=set()
    match_rules=parse_match(match_rules)
    #print 'match_rules',match_rules
    nodes=set(match_rules['node'].split('|'))    
    
    # nodes=all_nodes    
    for i in range(10):
        tnodes,tedges=get_one_level_upstream(nodes,match_rules['direct'],fedges,bedges)        
        nodes.update(tnodes)
        edges.update(tedges)
    if debug:
        print 'edges:',edges
        print 'nodes:',nodes
    return nodes,edges
    
def get_one_level_upstream(nodes,direction,fedges,bedges):
    tnodes=set()
    tedges=set()
    if direction=='down':
        edges=fedges
    else:
        edges=bedges
    for node in nodes:
        tnodes.add(node)
        if node not in edges:            
            #print '[Not found Edge node]',node,direction
            continue
        for tnode in edges[node]:            
            tnodes.add(tnode)
            if direction=='down':
                tedges.add('%s-%s'%(node,tnode))
            else:
                tedges.add('%s-%s'%(tnode,node))
    return tnodes,tedges
    
hanshu={}
def draw_a_node_set(all_nodes,fedges,bedges):
    for k,v in hanshu.items():
        match_rules='node:%s,direct:%s'%('|'.join(v),'up')
        #print 'match_rules string',match_rules
        nodes,edges=query_nodes(fedges,bedges,match_rules)
        # draw_graph(nodes,edges,'ads_1111_%s'%k )
    
def draw_up_down(all_nodes,fedges,bedges,out_dir='%s_flow/%s'):    
    # return
    for direct in ('up','down'):
        for node_target in all_nodes:
            match_rules='node:%s,direct:%s'%(node_target,direct)
            nodes,edges=query_nodes(fedges,bedges,match_rules)
            if len(nodes)==1:
                continue
            # draw_graph(nodes,edges,'%s_svg/%s'%(direct,node_target) )
            #print 'draw node:',node_target
            plot_info={'plot_type':direct,'table_name':node_target}
            path=out_dir%(direct,node_target)
            folder = os.path.split(path)[0]
            if not os.path.isdir(folder):
                print 'os.makedirs(%s)'%folder
                os.makedirs(folder)
            draw_cytoscape(nodes,edges,path ,plot_info=plot_info)

# print nodes
def main():
    if len(sys.argv)>=3:
        fname=sys.argv[1]
        cluster=sys.argv[2]
        # fname="azk_dep_info.xls"
        all_nodes,fedges,bedges=load_hql_meta(fname)
        draw_up_down(all_nodes,fedges,bedges,cluster+'_%s_flow/%s')
        # draw_a_node_set(all_nodes,fedges,bedges)
    else:
        fname=DATA_FILE
        # fname="azk_dep_info.xls"
        all_nodes,fedges,bedges=load_hql_meta(fname)
        draw_up_down(all_nodes,fedges,bedges,'%s_flow/%s')
        # draw_a_node_set(all_nodes,fedges,bedges)
    
if __name__=='__main__':            
    main()
