'''
#if vars_list['refer'].startswith('http%3A') or vars_list['refer'].startswith('https%3A'):
#   vars_list['refer'] = unquote(vars_list['refer'])
vars_list['refer'] = vars_list['refer'].replace("\t","")
vars_list['refer'] = re.sub(r"url=[^&]+&","",vars_list['refer'])
vars_list['refer'] = re.sub(r"&eqid=[^&]+","",vars_list['refer'])
vars_list['refer'] = re.sub(r"&ts=[^&]+","",vars_list['refer'])
vars_list['refer'] = re.sub(r"&t=[^&]+","",vars_list['refer'])
'''
import urllib

def recurse_unquote(url):
    nurl=url
    cnt = 0
    for i in range(10):
        if nurl[:10].find('%')>0:
            nurl = urllib.unquote( nurl )
            cnt += 1
        else:
            break
    return nurl,cnt
    
def kv_split(st,asep,ksep):
    rlst = []
    for pair in st.split(asep):
        arr =pair.split(ksep)
        key = arr[0]
        value = arr[1] if len(arr)>1 else ''
        rlst.append((key,value))
    return rlst
    
def remove_parameter(url,lst):
    uarr = url.split('?')
    base = uarr [0]    
    qstr = uarr[1] if len(uarr)>1 else ''
    qlst = kv_split(qstr,'&','=')
    nqlst = []    
    for qtup in qlst:
        if qtup[0] not in lst:
            nqlst.append(qtup)    
    nqstr=''
    if len(nqlst) > 0:
        nqstr='?'+'&'.join(['='.join(tp) for tp in nqlst])
    nurl = base + nqstr    
    return nurl
    
def keep_parameter(url,lst):
    uarr = url.split('?')
    base = uarr [0]    
    qstr = uarr[1] if len(uarr)>1 else ''
    qlst = kv_split(qstr,'&','=')
    nqlst = []    
    for qtup in qlst:
        if qtup[0] in lst:
            nqlst.append(qtup)    
    nqstr=''
    if len(nqlst) > 0:
        nqstr='?'+'&'.join(['='.join(tp) for tp in nqlst])
    nurl = base + nqstr    
    return nurl
    
def main():
    urls=[    'http://www.baidu.com/s?word=%E6%AC%A7%E5%A7%86%E8%B0%83%E9%9B%B6%E7%9A%84%E5%9B%BE%E7%89%87&tn=site888_3_pg&lm=-1&ssl_s=0&ssl_c=ssl1_14eca94d110',
    ]    
    for url in urls:
        print url
        print remove_parameter(url,['word'])
        print keep_parameter(url,['word'])
        
if __name__ == '__main__':
    main()