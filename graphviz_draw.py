#coding:utf8
from __future__ import unicode_literals 
import pygraphviz as pgv
import webbrowser
import optparse 

g_dic=[
['wise_dimension.sh',
 'udw:wiseps_query_click,udw:ud_tl_wise_insight_hour,file:data/pvuv,file:data/ch_ua,wise_top_query',
 '0:0-0:2,0:1-0:3,0:1-0:4'],
['wise_ua_stat.sh',
 'udw:wap_iknow...,hdfs_txt:wise_zhidao_refer,file:wise_ua_stat',
 '1:0-1:1,1:1-1:2'],
['wise_market_share_refer',
 'holmes_pre_session_off_for_ps_ubs,hdfs_txt:wise_share_middle,udw:ud_tl_wise_ps_se_query_type_trade,hdfs_txt:wise_share_middle_newtrade,total_forBA,total_forBA_newtrade',
 '2:0-2:1,2:1-2:3,2:2-2:3,2:1-2:4,2:3-2:5'],
['wise_market_share_clickprob',
'inputwords,spider_pages,rank_data,baidu_clickrate,click_prob,other_fixrate',
'3:0-3:1,3:1-3:2,0:0-3:3,3:2-3:4,3:3-3:4,3:5-3:4,0:0-3:0'],
['wise_market_share',
  'wise_market_share,mod_matrix_2',
'2:4-4:0,3:4-4:0,4:1-4:0'],
['wise_dapan_datafix',
  'browser_fix,brand_fix,os_fix,request_fix,palo:wise_channel_dim',
  '4:0-5:0,0:2-5:0,4:0-5:1,0:2-5:1,4:0-5:2,0:2-5:2,4:0-5:3,5:4-5:3,1:2-5:0,1:2-5:1,1:2-5:2,2:5-5:3',
]

]

def draw_graph(g_dic):
    for row in g_dic:
        row[1]=row[1].split(',')
        row[2]=row[2].split(',')
    print g_dic
        
    G=pgv.AGraph(strict=False, directed=True)
    for g_name,nodes,edges in g_dic:
        sg=G.add_subgraph(name='cluster:%s'%g_name,label=g_name)
        for node in set(nodes):
            sg.add_node(node)
    for g_name,nodes,edges in g_dic:
        for edge in set(edges):
            pair=edge.split('-')
            snode=get_node(g_dic,pair[0])
            enode=get_node(g_dic,pair[1])
            sg.add_edge(snode,enode)
    print G
    G.draw('test.jpg', prog='dot') 
    webbrowser.open('test.jpg')

def get_node(g_dic,pos):
    pos_arr=pos.split(':')
    pos_arr.insert(1,1)
    pt=g_dic
    for pos in pos_arr:
        # print pt ,pos_arr
        pos=int(pos)
        pt=pt[pos]
    return pt
    
if __name__=='__main__':
    draw_graph(g_dic)