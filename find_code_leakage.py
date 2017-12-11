import os

def find_pitfall(fpath):
    for i,l in enumerate(open(fpath)):
        row=l.split()
        for el in row:
            if el.find('.com')!=-1:
                print fpath,i+1,l

for root,dirs,files in os.walk('.'):
    for file in files:
        fpath=os.path.join(root,file)
        # print fpath
        find_pitfall(fpath)
        