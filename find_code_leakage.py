import os
import re
pat=re.compile('.*\.com\W|AzkabanAPI')
def find_pitfall(fpath):
    if fpath.endswith('.js'):
        return
    for i,l in enumerate(open(fpath)):
        row=l.split()
        for el in row:
            m=pat.match(el)
            if m is not None:
                print fpath,i+1,l

for root,dirs,files in os.walk('.'):
    for file in files:
        fpath=os.path.join(root,file)
        # print fpath
        find_pitfall(fpath)
        