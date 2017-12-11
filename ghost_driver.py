from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from multiprocessing import Pool   
import threading    
   
PROXY = "localhost:8580"
service_args = ['--proxy='+PROXY, '--proxy-type=http']
service_args = []
dcap = dict(DesiredCapabilities.PHANTOMJS)
dcap["phantomjs.page.settings.userAgent"] = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:50.0) Gecko/20100101 Firefox/50.0")
# driver = webdriver.PhantomJS(executable_path="D:\\code\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe",service_args=service_args, desired_capabilities=dcap)  
driver = webdriver.PhantomJS(executable_path="D:\\code\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe", desired_capabilities=dcap)  
# driver = webdriver.Firefox(executable_path="C:\\Program Files (x86)\\Mozilla Firefox\\firefox.exe")  
# driver = webdriver.Chrome(executable_path="C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe")  

def mpage_click(n,cnt=4):
    for i in range(cnt):
        driver.get("http://mobi.mconnect.cn/codetest/")
        driver.save_screenshot('screenie.png')
        driver.find_element_by_id("bt1Div").click()
        driver.find_element_by_id("bt2Div").click()
        for ck in driver.get_cookies():
            print 'group:',n,ck['name'],ck['value']
        data = driver.title
        print data.encode('gbk')
        driver.delete_all_cookies()
        
def b():
    driver = webdriver.PhantomJS(executable_path="D:\\code\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe")  
    driver.get("http://www.baidu.com")
    for ck in driver.get_cookies():
        print ck['name'],ck['value']
    print ''
   
def main():   
    mpage_click(1,1)
 
def multi_p():
    p=Pool(4)
    print(p.map(mpage_click, range(1)))
    
def multi_t():
    threads=[]    
    for i in range(1):
        th=threading.Thread(target=mpage_click, args=[i,5])
        threads.append(th)
        th.start()    
    
    
if __name__=='__main__':
    try:
        main()
        # multi_p()
        # multi_t()
    finally:
        driver.close()