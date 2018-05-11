#! -*- coding:utf8 -*-
import requests
import json
#http://api.map.baidu.com/library/CityList/1.4/examples/CityList.html
'''
http://api.map.baidu.com/shangquan/forward/?qt=sub_area_list&ext=1&level=1&areacode=332&business_flag=0&callback=BMap._rd._cbk13595

http://api.map.baidu.com/shangquan/forward/?qt=sub_area_list&ext=1&level=1&areacode=1454&business_flag=1&callback=BMap._rd._cbk79693
'''


def prov_list():
    s=u'''<option value="131" title="北京市">北京市</option>
<option value="289" title="上海市">上海市</option>
<option value="332" title="天津市">天津市</option>
<option value="132" title="重庆市">重庆市</option>
<option value="2" title="黑龙江省">黑龙江省</option>
<option value="6" title="甘肃省">甘肃省</option>
<option value="7" title="广东省">广东省</option>
<option value="8" title="山东省">山东省</option>
<option value="9" title="吉林省">吉林省</option>
<option value="10" title="山西省">山西省</option>
<option value="11" title="青海省">青海省</option>
<option value="12" title="新疆维吾尔自治区">新疆维吾尔自治区</option>
<option value="13" title="西藏自治区">西藏自治区</option>
<option value="15" title="湖北省">湖北省</option>
<option value="16" title="福建省">福建省</option>
<option value="17" title="广西壮族自治区">广西壮族自治区</option>
<option value="18" title="江苏省">江苏省</option>
<option value="19" title="辽宁省">辽宁省</option>
<option value="20" title="宁夏回族自治区">宁夏回族自治区</option>
<option value="21" title="海南省">海南省</option>
<option value="22" title="内蒙古自治区">内蒙古自治区</option>
<option value="23" title="安徽省">安徽省</option>
<option value="24" title="贵州省">贵州省</option>
<option value="25" title="河北省">河北省</option>
<option value="26" title="湖南省">湖南省</option>
<option value="27" title="陕西省">陕西省</option>
<option value="28" title="云南省">云南省</option>
<option value="29" title="浙江省">浙江省</option>
<option value="30" title="河南省">河南省</option>
<option value="31" title="江西省">江西省</option>
<option value="32" title="四川省">四川省</option>
<option value="2911" title="澳门特别行政区">澳门特别行政区</option>
<option value="2912" title="香港特别行政区">香港特别行政区</option>
<option value="9000" title="台湾省">台湾省</option>'''
    prov_list={}
    for l in s.splitlines():
        row= l.split('"')
        aid=row[1]
        name=row[3]
        # print '[load_prov]',aid,name.encode('gbk')
        prov_list[aid]={'parent_code':0,'name':name,'atype':1,'data':{}}
    return prov_list
    




def st_jsn(jsn,ecd='utf8'):
    st = json.dumps(jsn,indent=2,ensure_ascii=False).encode(ecd)
    return st
    
def prt_jsn(jsn):
    print st_jsn(jsn,'gbk')
   
    
def get_url(url):
    r=requests.get(url)
    jsn=r.json()
    return jsn
    
s_url='http://api.map.baidu.com/shangquan/forward/?qt=sub_area_list&ext=1&level=1&areacode={0}&business_flag=0'
def get_sub_area(query_id):
    jo = get_url(s_url.format(query_id))
    res={}
    if 'content' not in jo:
        return res
    if 'sub' not in jo['content']:
        return res
    for sub in jo['content']['sub']:
        area_code=sub['area_code']
        atype=sub['area_type']
        if atype!='3':
            sres=get_sub_area(area_code)
        res.update(sres)
        info={'parent_code':str(query_id),'atype':'3','name':sub['area_name'],'data':sub}
        res[area_code]=info
    return res
    
    
q_url='http://api.map.baidu.com/shangquan/forward/?qt=sub_area_list&ext=1&level=1&areacode={0}&business_flag=1'    
def get_quan(said):
    jo = get_url(q_url.format(said))
    # prt_jsn(jo)
    res={}
    if 'content' not in jo:
        return res
    if 'sub' not in jo['content']:
        return res
    for sub in jo['content']['sub']:
        row={'name':sub['area_name'],'parent_code':str(said),'data':sub}
        res[sub['area_code']]=row
    return res    
    
def crawl_area():
    is_break=False
    load_json=True
    prov = prov_list()
    area={}
    quan={}
    if load_json:
        jo= json.load(open('area.json'))
        return jo['area'],jo['prov']
    for aid in prov:
        # aid ='7'
        # aid ='131'        
        print '[get_sub_area]',aid
        subs = get_sub_area(aid)
        area.update(subs)
        if is_break:
            break
    fh=open('area.json','w')
    ainfo={'prov':prov,'area':area}
    st=st_jsn(ainfo)
    fh.write( st )
    fh.close()
    return area,prov
    
def crawl():
    is_break=False    
    area,prov=crawl_area()    
    for id,info in area.items():
        if info['atype']=='3':            
            print '[crawl]',id,info['name'].encode('gbk')
            res = get_quan(id)
            quan.update(res)
        if is_break:
            break
    fh=open('quan.json','w')    
    st=st_jsn(quan)
    fh.write( st )
    fh.close()
        
def export():
    jo=json.load(open('area.json'))    
    prov=jo['prov']
    area=jo['area']
    shangquan=json.load(open('quan.json'))
    
    print len(shangquan)
    fh=open('sq_gps.txt','w')
    for s,info in shangquan.items():
        obj=area[str(info['parent_code'])]
        aname= obj['name']
        pid=str(obj['parent_code'])
        aname1=''
        if pid not in prov:            
            obj = area[pid]            
            aname1=obj['name']
            pid=str(obj['parent_code'])
        if aname1=='':
            aname1=aname
            aname=''
        pname=prov[pid]['name']
        data= info['data']   
        st='\t'.join([ '%s'%s,pname,aname1,aname,info['name'],data.get('business_type',''),data.get('business_geo','') ])
        print>>fh, st.encode('gbk')
        
    
if __name__=='__main__':
    # crawl()
    export()


