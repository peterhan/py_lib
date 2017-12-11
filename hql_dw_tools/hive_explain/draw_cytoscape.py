cyto_temp=open('cyto_tmplt.html').read()
def draw_cytoscape(nodes,edges,outfile,info=None,sep='-'):
    tmplt_wrap='''elements: {
        nodes: [
            %s							
        ],
        edges: [
            %s
        ]                        
    },'''
    jsarr=[]
    for node  in nodes:
        label=info.get(node,'') if info is not None else ''
        label='\\n'.join(label).replace('\n','\\n')
        elm="{data:{id:'%s',label:'%s'}}"%(node,node+'\\n'+label)
        jsarr.append(elm)
    js_nodes=',\n'.join(jsarr)
    # print js_nodes
    sp_edges=[edge.split(sep) for edge in edges]
    js_edges=',\n'.join(["{data:{source:'%s',target:'%s'}}"%(edge[0],edge[1]) for edge in sp_edges if edge[0]!=edge[1]])
    # print js_edges
    data_js= tmplt_wrap%(js_nodes,js_edges)
    fname = outfile+'.html'
    with open(fname,'w') as fh:
        fh.write(cyto_temp.replace('{data_js_injection}',data_js))
    return fname