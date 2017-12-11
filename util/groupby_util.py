def count(lst):
    return len(map(int, lst ))
    
def d_count(lst):
    return len(map(int, set(lst) ))
    
def concat(lst):
    return ','.join(map(str, lst))

def d_concat(lst):
    return ','.join(map(str, set(lst) ))

def avg(lst):
    return _sum(ilst)*1.0/len(lst)
    
def _sum(lst):
    ilst=map(int,lst)
    return sum(ilst)
    
FUNC_TABLE={'sum':_sum,'avg':avg,'count':count,'d_count':d_count,'concat':concat,'d_concat':d_concat}

def kvsplit(st,asep,ksep):
    dc=[]
    for pair in st.split(asep):
        arr=pair.split(ksep)
        key=arr[0]
        value=arr[1] if len(arr)>1 else ""
        dc.append([key,value])
    return dc

def parse_group_option(groupbykey,selectstring):
    select_dict=kvsplit(selectstring,',',':')
    outcols = map(lambda x:int(x[0])-1,select_dict )
    aggfuncs = map(lambda x:str(x[1]),select_dict )
    keycols = map(lambda x:int(x)-1,groupbykey.split(' '))
    return select_dict,outcols,aggfuncs,keycols
    
def print_result(out_table):
    for k,v in out_table.items():
        print '%s\t%s'%( k,'\t'.join(map(str,v)) )
        
def groupby(dataset,groupbykey,selectstring):
    select_dict,outcols,aggfuncs,keycols = parse_group_option(groupbykey,selectstring)
    value_table = {}
    out_table = {}
    for row in dataset:
        key='\t'.join([ '%s'%row[col] for col in keycols ])
        aggdata = [[] for i in outcols]
        value_table.setdefault(key,aggdata)
        for idx,col in enumerate(outcols):
            value_table[key][idx].append(row[col])
    for key,dataset in value_table.items():
        # print key,dataset
        outrow=[]
        for idx,col in enumerate(outcols):
            func=FUNC_TABLE[aggfuncs[idx]]
            outrow.append(func(value_table[key][idx]))
        out_table[key]=outrow

    return out_table
        

        
ds=[
[1,2,3,4],
[1,2,3,4],
[2,2,3,4],
]        
a=groupby(ds,'1','2:d_count,3:sum,3:concat,3:d_concat')
print_result(a)

    