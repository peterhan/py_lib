import requests
import bs4

idxurl='http://www.se-radio.net/page'

def u2g(s):return s.encode('gbk','ignore')

def parse_index(i):
    rq=requests.get(idxurl+'/%s/'%i)
    html=rq.text
    # print u2g(html)
    soup=bs4.BeautifulSoup(html)
    tags=soup.findAll('a',class_='more-link')
    # tags=soup.findAll('a')
    return [tag['href'] for tag in  tags]
    
# print parse_index(1)
def down_index():
    res=[]
    for i in range(1,48): 
        # print i
        res.extend(parse_index(i))    
    for url in res:
        print url
        
def parse_soundtrack(url):
    rq=requests.get(url)
    html=rq.text
    # print u2g(html)
    soup=bs4.BeautifulSoup(html)
    tags=soup.findAll('a')
    # print tags
    return [ tag['href'] for tag in  tags if tag['href'].endswith('.mp3') ] [0]

for i,line in enumerate(open('se-radio-url.list') ):
    url=line.strip('\n')
    if i<112:
        continue
    try:print parse_soundtrack(url)
    except:print 'ERROR: 'url