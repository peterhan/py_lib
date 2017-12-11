import email
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import encoders

import base64
import httplib
import mimetypes
import smtplib

class SendMail:

    def create_mail(self, from_addr='hanshu@baidu.com', to_addr=[], cc_addr=[], subject='', body=''):
        self.mail = MIMEMultipart()
        self.mail['From'] = from_addr
        self.mail['To'] = '; '.join(to_addr)
        self.mail['Cc'] = '; '.join(cc_addr)
        self.mail['Subject'] = "=?UTF-8?B?"+base64.b64encode(subject)+"?="
        self.mail.attach(MIMEText(body, 'html', 'UTF-8'))

        self.from_addr = from_addr
        self.to_addr = to_addr + cc_addr

    def add_attachment(self, files):
        for attachment in files:
            ctype, encoding = mimetypes.guess_type(attachment)
            maintype, subtype = ctype.split('/',1)

            fp = open(attachment, 'rb')
            msg = MIMEBase(maintype, subtype)
            msg.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            self.mail.attach(msg)

    def send_mail(self, host='mail.baidu.com', username='hanshu', password=''):
        s = smtplib.SMTP(host, 587)
        s.starttls()
        s.login(username, password)
        s.sendmail(self.from_addr, self.to_addr, self.mail.as_string())
        s.quit()
        
        
sendmail = SendMail()
sendmail.create_mail('hanshu@baidu.com', ['hanshu@baidu.com'], [], 'testest', 'testbody')
res=sendmail.send_mail()
print res