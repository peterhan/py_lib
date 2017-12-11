import re,glob
import sys,os

fn=sys.argv[1] if len(sys.argv)>1 else glob.glob('*.sql')[-1]
print 'read file:',fn

parse_obj={
's1_not_s2':'# WARNING: Objects in server1.dw_ec_order_etl but not in server2.dw_ec_order_etl:',
's2_not_s1':'# WARNING: Objects in server2.dw_ec_order_etl but not in server1.dw_ec_order_etl:',
'diff_list':'#\sType\s*Object\s*Name\s*Diff\s*Count\s*Check\s*',
'obj_item':'#\s+(\w+):\s(\w+)',
'obj_fail':'#\s+(\w+)\s+(\w+)\s+FAIL\s*',
}
for k in parse_obj:parse_obj[k]=re.compile(parse_obj[k])

thiskey='default'
obj_list={}
for l in open(fn):
    l= l.strip()
    for key,pat in parse_obj.items():
        m=pat.match(l)
        if not m:
            continue
        # print '[%s]'%key,l
        if key.startswith('obj_'):            
            obj_type=m.group(1)
            obj_name=m.group(2)
            obj_list.setdefault(thiskey,[])
            obj_list[thiskey].append([obj_type,obj_name])
        else:
            thiskey=key
        break


templates={
's1_not_s2':'{},{}\n',
's2_not_s1':'drop {0} if exists  {1};\n',
'diff_list':'{},{}\n',
'obj_item':'{},{}\n',
'obj_fail':'{},{}\n',
'default':'{},{}\n'
}
def get_whitelist(argv):
    whitelist=[]
    fn='../full_white_list.txt' if len(argv)<2 else argv[1]
    print 'read white list from:',fn
    for l in open(fn):
        whitelist.append(l.strip().strip('#'))
    return whitelist
    

for k in  obj_list:
    lst=obj_list[k]
    fname=k+'.txt'
    with open(fname,'w') as fh:
        print fname
        for row in lst:
            fh.write(templates[k].format(row[0],row[1]))
            
wl=get_whitelist(sys.argv)
for e in obj_list['s2_not_s1']:
    if e in wl:
        print e