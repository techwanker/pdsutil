import pdsutil.DbUtil as Dbutil
from pdsutil.DbUtil import SqlStatements, ConnectionHelper, CursorHelper
import datetime
import psycopg2
import yaml

class isql:

    def __init__(self):
        self.statements = SqlStatements.from_statement_list([])
        print(self.statements)
        self.statement = None
        self.connection = None
        self.cursor = None
        self.binds = {}

    def dump_statements(self):
        print ("about to dump")
        print(yaml.dump(self.statements))

    def connect(self,name:str):
        self.connection = ConnectionHelper().get_named_connection(name)
        self.cursor = CursorHelper(self.connection.cursor())

    def load(self,filename:str):
        self.statements = SqlStatements.from_yaml(filename).statements
        self.list_statements()

    def list_statements(self,verbose=False):
        for i, v in enumerate(self.statements):
            print ("%s %s" % (i, v))
        # for k in self.statements:
        #     print (k)

    def bind_date(self,name,year, month, day):
        self.binds[name] = datetime.datetime(year,month,day)

    def use(self,number:int):
        for i, k in enumerate(self.statements):
            if i == number:
                self.statement = self.statements[k]


    def use_statement(self,name:str):
        self.statement = name

    def run(self,index=None):
        if index == None:
            print("stmt: %s" % self.statement)
        else:
            for i, k in enumerate(self.statements):
                if i == index:
                    self.statement = self.statements[k]
                    print("k: %s sql: %s" % (k, self.statements["sql"]))
        rows = self.cursor.execute(self.statement["sql"],self.binds)
        for row in rows:
            print (row)


    def bind(self,name,value):
        self.binds[name] = value

    def execute(self,statement_name:str=None):
        if self.connection is None:
            print("use connect before use")
        if statement_name:
            sql = self.statements[statement_name]
        elif self.statement is None:
            sql = self.statements[statement_name]
        else:
            print ("pass statement or call use_statement() before execute")
        result = self.cursor.execute(sql,self.binds)

    import sys
    def to_csv(self,headers=True,outfile=sys.stdout):
        import csv
        rows = self.execute()
        column_names  = [i  for i[0] in self.cursor.description]
        print (column_names)

        writer = csv.writer(outfile, dialect="excel",
                            delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_NONNUMERIC)
        if headers:
            writer.writerow(column_names)
        for row in rows:
            writer.writerow(row)

    @staticmethod
    def help():
        print("connect()")
        print ("list_statements()")
        print ("execute(statement_name)")
        print ("bind_date(name,yr,month,day)")
        print ("load('filename')")
        print ("use statement(statement_name")
        print ("use")

i = isql()
connect = i.connect
help = i.help
execute = i.execute
load = i.load
list_statements = i.list_statements
bind_date = i.bind_date
use_statement= i.use_statement
dump_statements = i.dump_statements
use = i.use
run = i.run
bind = i.bind
list = i.list_statements
