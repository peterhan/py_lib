class SessionContainer:
    '''
    Set this object's method as you need

    # get key from row
    sc.get_key = lambda row:row[0]

    # method to judge current record is still in session
    sc.is_same_session = lambda row,rows: True        

    # method to discard final output record
    sc.is_output = lambda rows:True

    # method to output all rows in last session
    sc.output_rows = lambda rows: self.prt(' | '.join(':'.join(row) for row in rows) )
    '''
    def prt(self,st): 
        print st
        
    def __str__(self):
        return '\n'.join( [str(row) for row in self.rows])+'\n'
        
    def __init__(self):        
        self.defaultkey = '__sessionContainer_defaultkey__'
        self.last_key = self.defaultkey
        self.rows = []
        self.get_key = lambda row:row[0]
        self.is_same_session = lambda row,rows: True        
        self.is_output = lambda rows:True
        self.output_rows = lambda rows: self.prt(' | '.join(':'.join(row) for row in rows) )
        
    def add_row(self,row):
        key = self.get_key(row)
        if key == self.last_key \
           and self.is_same_session(row,self.rows):
            self.rows.append(row)
        else:
            self.finish_session()            
            self.rows.append(row)
            self.last_key = key
        
    def close(self):
        self.finish_session()
        
    def finish_session(self):
        if not self.is_output(self.rows):
            return
        if self.last_key == self.defaultkey:
            return
        self.output_rows(self.rows)
        self.rows = []
        

class GroupSessionContainer(SessionContainer):
    '''
    Set this object's method as you need
       
    ## get group by key, 
    .get_group_key = lambda row:row[0]
    
    ## a function compare row with group_rows decide current row is in group    
    .is_group = lambda row,rows:True
    
    ## for group rows loop's order see the python's sorted method
    .group_sort_cmp = None  #
    .group_sort_key =  None  #
    .group_sort_reverse =  False
    
    ### inherit method option
    # get key from row
    sc.get_key = lambda row:row[0]

    # method to judge current record is still in session
    sc.is_same_session = lambda row,rows: True        

    # method to discard final output record
    sc.is_output = lambda rows:True

    # method to output all rows in last session
    sc.output_rows = lambda rows: self.prt(' | '.join(':'.join(row) for row in rows) )
    '''
    def __init__(self):
        SessionContainer.__init__(self)
        self.defaultgroupkey = '__groupSessionContainer_defaultkey__'
        self.last_group_key = self.defaultgroupkey
        self.group_rows = []
        self.get_group_key = lambda row:row[0]
        self.is_group = lambda row,rows:True
        self.group_sort_cmp = None  #
        self.group_sort_key =  None  #
        self.group_sort_reverse =  False
        
    def __str__(self):
        return '\n'.join( [str(row) for row in self.group_rows])+'\n'
        
    def add_row(self,row):
        key = self.get_group_key(row)
        if key == self.last_group_key and self.is_group(row,self.rows):
            self.group_rows.append(row)
        else:
            self.finish_group()
            self.group_rows.append(row)
            self.last_group_key = key
        
    def close(self):
        self.finish_group()
        
    def finish_group(self):
        # print self
        #finish logic
        sc = SessionContainer()
        sc.get_key = self.get_key
        sc.is_same_session = self.is_same_session
        sc.is_output = self.is_output
        sc.output_rows = self.output_rows
        #
        sorted(self.group_rows,
            cmp=self.group_sort_cmp ,
            key=self.group_sort_key,
            reverse=self.group_sort_reverse)
            
        for row in self.group_rows:
            sc.add_row(row)
        sc.close()
        self.group_rows=[]        
        

#os.environ('mapreduce_map_input_file')    
def testSessionContainer(rows):
    so = SessionContainer()
    def myis_same_sess(row,rows):
        if int (row[-1]) - int(rows[-1][-1]) >3:
            return False
        return True
    so.is_same_session = myis_same_sess
    # so.rows_result = rows_result
    for row in rows:
        so.add_row(row.split(' '))
        print so
    so.close()
    
def testGroupSessionContainer(rows):
    gso = GroupSessionContainer()
    def myis_same_sess(row,rows):
        if int (row[-1]) - int(rows[-1][-1]) >3:
            return False
        return True
    gso.is_same_session = myis_same_sess
    # so.rows_result = rows_result
    for row in rows:
        gso.add_row(row.split(' '))
        # print so
    gso.close()
    
if __name__ == '__main__':
    rows = [
     '1 1 1 2',
     '1 1 2 100',
     '1 3 3 3',
     '1 4 4 1',
     '1 4 5 3',
     '2 5 5 2',
     '2 5 4 3',
     '2 6 5 2',
     '2 5 5 1'
    ]
    # testSessionContainer(rows)
    testGroupSessionContainer(rows)
