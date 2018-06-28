import MySQLdb

class MySQLdb_Connection:
    
    def __init__(self, host, user, passwd, db, port=3306):
        self.con = MySQLdb.connect(host, user, passwd, db, port=port)
        cur = self.con.cursor()
        self.cur = cur

    def query_meta(self, sql):
        count = self.cur.execute("set names utf8;")
        count = self.cur.execute(sql)
        self.num_fields = len(self.cur.description)
        self.field_names = [i[0] for i in self.cur.description]

    def query(self, sql, as_dict=False):
        try:
            count = self.cur.execute("set names utf8;")
            count = self.cur.execute(sql)
            self.num_fields = len(self.cur.description)
            self.field_names = [i[0] for i in self.cur.description]
            results = self.cur.fetchall()
            for row in results:
                # print '\t'.join(row)
                if as_dict:
                    yield dict(zip(self.field_names,row))
                else:
                    yield row
        except MySQLdb.Error, e:
            print e.message
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)

    def executemany(self, sql, rows):
        self.cur.executemany(sql, rows)
        self.con.commit()
        rows_affected = self.cur.rowcount
        return rows_affected
        

    def execute(self, sql):
        self.cur.execute(sql)
        self.con.commit()
        rows_affected = self.cur.rowcount
        return rows_affected

    def execute_args(self, sql,args):
        self.cur.execute(sql,args)
        self.con.commit()
        rows_affected = self.cur.rowcount
        self.cur.close()
        self.cur=self.con.cursor()
        return rows_affected
        
    def close(self):
        if self.con:
            self.con.close()