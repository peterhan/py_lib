import sqlite3
import base64

class ConfStore(dict):
    def __init__(self,fname=None):
        if fname is None:
            fname='./ConfStore.db'
        self.fname=fname
        self.execute('create table  if not exists conf_store  (key text,value text);')
        
    def encode(self,input):
        return base64.b64encode(''.join(reversed(input)) )
    
    def decode(self,input):
        return ''.join(reversed(base64.b64decode(input) ))
    
    def execute(self,*args):
        conn=sqlite3.connect(self.fname)
        conn.isolation_level = None
        c=conn.cursor()
        c.execute(*args)
        row=c.fetchone()
        conn.commit()
        conn.close()
        return row
       
    def __contains__(self,key):
        row= self.execute('select 1 from conf_store where key=?',key)        
        if row is None:
            return False
        return True
        
    def set(self,key,value):        
        evalue=self.encode(value) 
        # print evalue
        if key not in self:
            self.execute('insert into conf_store values(?,?);',[key,evalue])
        else:
            self.execute('update conf_store set value =? where key=?;',[evalue,key])
        return value
        
    def __getitem__(self,key):
        row=self.execute('select key,value from conf_store where key=?;',key)
        if row is None:
            return None
        try:
            print row[1]
            return self.decode(row[1])
        except Exception as e:
            print e
            return None
    
    def get(self,key):
        return self[key]        
    
        
        
if __name__=='__main__':
    cs=ConfStore()
    print cs.set('a','ddd')
    print cs['a']