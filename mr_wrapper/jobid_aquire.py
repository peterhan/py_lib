import requests
r=requests.get(
'http://szwg-hadoop-k1460.szwg01.baidu.com:8045/jobdetails_DAG.jsp?jobid=%s'%'job_20150619172251_1607793')
print r.text