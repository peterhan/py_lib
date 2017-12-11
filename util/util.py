def iter_stream(stream,fields=1,sep='\t'):
    for l in stream:
        row = l.strip('\n').split(sep)
        if len(row)>=fields:
             yield row

def kv_split(st,asep,ksep):
    rlst = []
    for pair in st.split(asep):
        arr =pair.split(ksep)
        key = arr[0]
        value = arr[1] if len(arr)>1 else ''
        rlst.append((key,value))
    return rlst
    
def load_set(fname,func=lambda x:x):
    dc = set()
    for l in open(fname):
        row = l.strip('\n').split('\t')
        #if len(row)<2:continue
        dc.add(func(row[0]))
    return dc

def find_any(st,lst):
    for part in lst:
        if st.find(part)!=-1:
            return True
    return False
    
def is_utf8(st):
    #0~127
    #224~239 128~191 128~191
    l = len(s)
    i = 0
    while i<l:
        a = ord(s[i])
        if a >=0 and a<=127:
            i = i + 1
            continue
        elif a>=224 and a<=239 \
            and i+2<l \
            and ord(s[i+1])>=128 and ord(s[i+1])<=191 \
            and ord(s[i+2])>=128 and ord(s[i+2])<=191:
            i = i + 3
            continue
        else:
            return False
    return True
    
def load_dict(fname,min_len=2):
    dic = {}    
    for line in open(fname,'r'):
        row= line.strip('\n').split('\t')       
        if len(row) < min_len:
            continue
        dic[row[0].strip()] = row[1].strip()
    return dic
    
def domain_match(domain,dc):
    for key in dc:
        if domain.endswith(key):
            return True
    return False

def query_contain_any(query,dc):
    for key in dc:
        if query.find(key)>-1:
            #print key
            return True
    return False

BLACK_KW=["视频","电影","影视","高清"]
def remove_nocontent_part(query):
    mquery=query
    for kw in BLACK_KW:
        mquery=mquery.replace(kw,'')
    if mquery != query:print mquery,query
    return mquery