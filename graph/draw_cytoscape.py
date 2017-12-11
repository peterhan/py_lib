cyto_temp=open('cyto_tmplt.html').read()
def draw_cytoscape(nodes,edges,outfile):
    tmplt_wrap='''elements: {
        nodes: [
            %s							
        ],
        edges: [
            %s
        ]                        
    },'''
    js_nodes=',\n'.join(["{data:{id:'%s'}}"%node for node in nodes])
    # print js_nodes
    sp_edges=[edge.split('-') for edge in edges]
    js_edges=',\n'.join(["{data:{source:'%s',target:'%s'}}"%(edge[0],edge[1]) for edge in sp_edges if edge[0]!=edge[1]])
    # print js_edges
    data_js= tmplt_wrap%(js_nodes,js_edges)
    with open(outfile+'.html','w') as fh:
        fh.write(cyto_temp.replace('{data_js_injection}',data_js))