# import pygraphviz as pgv
import re
import webbrowser
from draw_cytoscape import draw_cytoscape

def export_json(nodes,edges,outfile,info):
    snodes={}
    slinkt={}
    for node in nodes: 
        label=node +'\n'+ '\n'.join(map(str,info[node]) )
        snodes.setdefault(node,{})
        snodes[node]['label']=label
    for i,node in enumerate(snodes):
        snodes[node]['idx']=i
        print "{id:'%s'},"%node
    for edge in edges:
        source,target=edge.split(' => ')
        print "{{source: nodes[{0}], target: nodes[{1}], left: false, right: true }},".format(snodes[source]['idx'],snodes[target]['idx'])
        
def draw_graph_with_info(nodes,edges,outfile,info):
    #circo dot fdp neato nop nop1 nop2 osage patchwork sfdp twopi
    G=pgv.AGraph(strict=False, directed=True,size = "10,10!",ratio="auto",layout="dot")
    # G=nx.Graph()
    g_name=outfile
    sg=G.add_subgraph(name='cluster:%s'%g_name,label=g_name)
    for node in nodes:        
        try:
            print info[node]
            label=node +'\n'+ '\n'.join(map(str,info[node]) )
            sg.add_node(node,label=label)
        except:print node
    for edge in edges:
        pair=edge.split(' => ')
        snode=pair[0]
        enode=pair[1]
        try:sg.add_edge(snode,enode)
        except:print 'Except Edge:',snode,enode
    # print G
    outfile=outfile+'.svg'
    G.draw(outfile, prog='neato') 
    webbrowser.open(outfile)
    
def leading_space_count(line):
    return len(line)-len(line.strip(' '))
    
def parse_explain_string(stat_str,temp_file='temp_explain.txt'):
    import StringIO
    parse_dict={}    
    for i,l in enumerate(StringIO.StringIO(stat_str)):
        line = l.strip('\n')
        ls_cnt =  leading_space_count(line)
        if ls_cnt==0 and len(line)!=0 and line.startswith('STAGE'):     
            parse_dict.setdefault(line,[])
            dc=parse_dict[line]
        else:
            dc.append(line)
    print 'parse_dict keys: ',parse_dict.keys()
    depend_info = parse_dict['STAGE DEPENDENCIES:']
    print '\n'.join(depend_info)
    stg_plan_text = parse_dict['STAGE PLANS:']
    # print '\n'.join(stg_plan)
    stg_info = parse_stage_plan(stg_plan_text)    
    # 
    parse_stage_depend_info(depend_info,stg_info,temp_file)
    
def parse_explain_file(fname):
    parse_dict={}    
    for i,l in enumerate(open(fname)):
        line = l.strip('\n')
        ls_cnt =  leading_space_count(line)
        if ls_cnt==0 and len(line)!=0 and line.startswith('STAGE'):     
            parse_dict.setdefault(line,[])
            dc=parse_dict[line]
        else:
            dc.append(line)
    print 'parse_dict keys: ',parse_dict.keys()
    depend_info = parse_dict['STAGE DEPENDENCIES:']
    print '\n'.join(depend_info)
    stg_plan_text = parse_dict['STAGE PLANS:']
    # print '\n'.join(stg_plan)
    stg_info = parse_stage_plan(stg_plan_text)    
    # 
    parse_stage_depend_info(depend_info,stg_info,fname)
    
    
def merge_consists(src):
    nsrc=[]
    ntgt=[]
    for i,elm in enumerate(src):
        if elm.startswith('consists of'):
            nst = ' '.join(src[i:]).split(' ')
            nst =[word.strip() for word in nst if word.startswith('Stage-')]
            ntgt.extend(nst)
            break
        else:
            nsrc.append(elm)
    return nsrc,ntgt

def parse_stage_depend_info(dep_info ,stg_info,fname):
    nodes = set()
    edges = set()
    for info in dep_info: 
        dep_st='depends on stages:'
        if info.find(dep_st)!=-1:
            tks = info.split(dep_st)
            target = tks[0].strip()
            nodes.add(target)
            src = map(lambda x:x.strip(),tks[1].split(','))
            nsrc,ntgt = merge_consists(src)
            # print nsrc,ntgt
            for snode in nsrc:
                edges.add('%s => %s'%(snode,target))
                nodes.add(snode) 
            for tnode in ntgt:
                edges.add('%s => %s'%(target,tnode))
                nodes.add(tnode) 
    # print nodes,edges
    # draw_graph_with_info(nodes,edges,'%s.Explain_Graph'%fname,stg_info)
    fname=draw_cytoscape(nodes,edges,'%s.Explain_Graph'%fname,info=stg_info,sep=' => ')
    webbrowser.open(fname)
    # export_json(nodes,edges,'%s.Explain_Graph'%fname,stg_info)
    return nodes,edges
        
def parse_stage_plan(stg_plan):
    stg_info = {}
    for i,l in enumerate(stg_plan):
        if l.find('Stage:')!=-1:
            name = l.split(' ')[-1]
            type = stg_plan[i+1].strip().replace(' ','_')
            tables = []
            info=''
            print 'parse_stage_plan', name ,type
            if type in ('Conditional_Operator'):
                pass
            elif type in ('Map_Reduce'):                 
                info = extract_stage_info(stg_plan,i)
                pass
            elif type in ('Map_Reduce_Local_Work'):                
                info = extract_stage_info(stg_plan,i)
                pass            
            stg_info[name] = (type,info)
    print 'parsed stage info:',stg_info.keys()
    return stg_info

STAT_PAT = re.compile('\s+Statistics: Num rows: (\d+) Data size: (\d+) Basic stats: \w+ Column stats: NONE')
def extract_stage_info(stg_plan,i):
    print 'parse stage from ',i
    tables = []
    stat = []
    last_spc_cnt = 0
    for j in range(1,1000):
        # print leading_space_count(stg_plan[i+j])
        
        if i+j>len(stg_plan)-1:
            continue
        if leading_space_count(stg_plan[i+j])<3:
            break
        l = stg_plan[i+j].strip()
        if l.startswith('STAGE:'):
            break
        if l.endswith('Operator') or l.find('TableScan')!=-1:
            if l.find('Fetch Operator')!=-1:
                continue
            type = l
            st = ''
            lastcnt=0
            for ln in stg_plan[i+j:i+j+50]:
                if leading_space_count(ln)<lastcnt:
                    break
                lastcnt=leading_space_count(ln)
                if ln.find('Statistics:')!=-1:
                    m=STAT_PAT.match(ln)
                    if m:
                        st += ' [count:%s, size:%s] '%m.groups()
                        break
                        # stat.append(st )
                if ln.find('alias:')!=-1:
                    st += '[alias:%s] '%ln.split(' ')[-1]
            stat.append( '[%s] '%type +': '+st)
        if l.startswith('alias:'):
            tables.append(l.split(' ')[-1])
    return '[InputTables: %s] '%','.join(tables)+'\n'+'\n'.join(stat)
    # return tables,''
    
if __name__=='__main__':
    import glob
    for fname in glob.glob('data/*.txt'):
        parse_explain_file(fname)