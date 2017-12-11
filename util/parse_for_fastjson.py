import json
import logging

def customize_type(obj):
    typ=type(obj)
    if isinstance(obj,unicode):typ='String'
    elif isinstance(obj,long):typ='Long'
    elif isinstance(obj,int):typ='Int'
    elif isinstance(obj,list):typ='ArrayList'
    # elif isinstance(obj,Boolean):typ='Boolean'
    return typ

def peek_json_obj(context,jobj,n=0):
    indent=' '*4*n
    if type(jobj)==dict:
        context['__jtype']='obj'
        for key,vlu in jobj.items():
            logging.debug(indent,'[Field]:',key,'[Type]:',customize_type(vlu)) 
            context.setdefault(key,{'__jtype':customize_type(vlu)})
            peek_json_obj(context[key],vlu,n+1)
    elif type(jobj)==list:
        for elm in jobj[:1]:
            peek_json_obj(context,elm,n+1)
    elif isinstance(jobj,basestring):            
        logging.debug(indent,'[String example]:"%s"'%jobj)
        
def classlize(cname):
    return cname.capitalize().strip('s')
    
def make_getter_setter(dic):
    name=dic['name'].capitalize()
    dic['getter']='get%s'%name
    dic['setter']='set%s'%name 
    
def genJavaCode(context,RootName='Dataset'):   
    inner_code=[]
    f_code=[]
    m_code=[]
    gs_template='''    
      public {d_type} {getter}() {{
        return {name};
      }}
      public void {setter}({d_type} {name}) {{
        this.{name} = {name};
      }}\n\n'''
    for name,value in context.items():
        if name=='__jtype':
            continue
        d_type=value.get('__jtype')
        classname=classlize(name)
        if d_type=='obj':
            #
            inner_code.append( genJavaCode(context[name],RootName=classlize(name)) )
            #
            data={'name':name,'d_type':'List<%s>'%classname,'d2_type':'ArrayList<%s>'%classname}
            make_getter_setter(data)
            code='private {d_type} {name}=new {d2_type}();\n'.format(**data)
            
        elif d_type=='ArrayList':
            data={'name':name,'d_type':'List<%s>'%'String','d2_type':'ArrayList<%s>'%'String'}
            make_getter_setter(data)
            code='private {d_type} {name}=new {d2_type}();\n'.format(**data)
        else:
            data={'name':name,'d_type':d_type}
            make_getter_setter(data)            
            code='private {d_type} {name};\n'.format(**data)
        f_code.append(code )
        # print data
        m_code.append(gs_template.format(**data) )    
    jcode='\npublic class %s{\n'%RootName
    for field in f_code:
        jcode+=' '*4+field
    for method in m_code:
        jcode+=method
    jcode+='\n}\n\n'
    for inner in inner_code:
        jcode+=inner
    return jcode
    
def test():
    jo = json.load(open('qczj-2017-03-07-log.json'))
    context={}
    peek_json_obj(context,jo)
    # print json.dumps(context,indent=2)
    rootclass='Dataset'
    code= '\n'+genJavaCode(context,rootclass)
    print code
    open(rootclass+'.java','w').write(code)
    
def main():
    import glob,os
    for fname in glob.glob('json/*.json'):
        farr= os.path.split(fname)
        rootclass=classlize(farr[-1].replace('.json',''))
        print rootclass
        context={}
        jo=json.load(open(fname))
        peek_json_obj(context,jo)
        code= '\n'+genJavaCode(context,rootclass)
        # print code
        open(rootclass+'.java','w').write(code)
        
if __name__=='__main__':
    main()
    