G_cyto_temp=open('cyto_tmplt.html').read()
def draw_cytoscape(nodes,edges,outfile,info=None,plot_info={},sep='-'):
    cyto_temp= G_cyto_temp
    tmplt_wrap='''\
    elements: {
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
    html_tbl='<table border="0.5">\n\n'
    row=[]
    for edge in sp_edges:
        if edge[0]==edge[1]:continue
        # row.append( '<tr><td>%s</td><td>=></td><td>%s</td></tr>\n'%(edge[0],edge[1]))
        row.append( '%s => %s \n'%(edge[0],edge[1]))
    html_tbl+=''.join(row)
    html_tbl+='\n\n</table>'
    # print js_edges
    data_js= tmplt_wrap%(js_nodes,js_edges)
    ##  file output
    fname = outfile+'.html'
    with open(fname,'w') as fh:
        cyto_temp = cyto_temp.replace('{data_js_injection}',data_js)
        cyto_temp = cyto_temp.replace('{html_tbl}',html_tbl)
        for key,vlu in plot_info.items():
            cyto_temp = cyto_temp.replace('{%s}'%key,vlu)
        fh.write(cyto_temp)
    return fname
