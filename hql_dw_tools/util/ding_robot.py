import sys
import urllib2

G_tok_conf={
'test_as_test':'8b35002bf89516e0ef4ac0f7fdad6cd5b2208cd6788c857f2477480233b9c379'
}

def send_ding_robot_tokname(msg,token_name):
    send_ding_robot(msg,G_tok_conf[token_name])

def send_ding_robot(msg,token):
    msg_data='''{
    "msgtype": "text", 
    "text": {
        "content": "%s"
    }, 
    "at": {
        "atMobiles": [
        ], 
        "isAtAll": false
    }
    }'''%(msg)
    # headers={"Content-Type":"application/json"}
    # r=requests.post(url,data=msg_data,headers=headers, verify=False)
    # print r.text
    
    url='https://oapi.dingtalk.com/robot/send?access_token='+token
    request = urllib2.Request(url,data=msg_data)
    request.add_header("Content-Type", "application/json")
    response = urllib2.urlopen(request)
    st= response.read()
    print st
    return st
   
if __name__=='__main__':
    msg='test'
    token_name='dw'
    if len(sys.argv)>2:
        msg=sys.argv[2]
        token_name=sys.argv[1]
    send_ding_robot(message,G_tok_conf[token_name])
