'''Overly complicated database copying since I don't want things to get messed
up if the web database is getting new data at the same time. '''
import psycopg2 as pg
from psycopg2 import sql
import subprocess
from secret import password

class DBCopier():
    def __init__(self,localdsn,remotedsn,table_name,dep_tables,foreign_keys):
        self.connfrom = pg.connect(remotedsn)
        self.connlocal = pg.connect(localdsn)
        self.curfrom = self.connfrom.cursor()
        self.curlocal = self.connlocal.cursor()
        self.table_name = table_name
        self.dep_tables = dep_tables
        self.foreign_keys = foreign_keys

    def checkforeign(self,row):
        curl1 = self.connlocal.cursor()
        curf1 = self.connfrom.cursor()
        for ii in range(len(dep_tables)):
            # Check if foreign key is already in local tables
            pk = [val for desc,val in zip(self.curfrom.description,row) if desc.name==self.foreign_keys[ii]]
            q_find = sql.SQL("SELECT COUNT(*) FROM {} WHERE {}={}").format(
                sql.Identifier(self.dep_tables[ii]),
                sql.Identifier(self.foreign_keys[ii]),
                sql.Placeholder()
            )
            curl1.execute(q_find,pk[0])
            present = curl1.fetchone()
            if present[0]:
                continue
            else: # Get corresponding entry from remote database
                q_get = sql.SQL("SELECT * FROM {} WHERE {}={}").format(
                    sql.Identifier(self.dep_tables[ii])
                    sql.Identifier(self.foreign_keys[ii])
                    sql.Placeholder()
                )
                curf1.execute()

    def copy(self,table_name,unique_name):
        self.curfrom.execute(sql.SQL("SELECT * FROM {};").format(sql.Identifier(table_name)))
        ncol = len(self.curfrom.description)

        for row in self.curfrom:
            self.checkforeign(row)
            q_ins = sql.SQL("INSERT INTO {} VALUES ({}) ON CONFLICT DO NOTHING;").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Placeholder()*ncol)
            )
            curlocal.execute(q_ins,row)
            connlocal.commit()

    if delete_remote:
        # Delete copied entry from remote database
        pk = [val for desc,val in zip(curfrom.description,row) if desc.name==unique_name] #key value corresponding to unique_name
        q_del = sql.SQL("DELETE FROM {} WHERE {}={};").format(
            sql.Identifier(table_name),
            sql.Identifier(unique_name),
            sql.Placeholder()
        )
        curfrom.execute(q_del,pk[0])
        connfrom.commit()
    curfrom.close()
    curlocal.close()
    connlocal.close()
    connfrom.close()

# Heroku remote database
# cprocess = subprocess.run("heroku config:get DATABASE_URL -a serene-river-35528",
#     stdout=subprocess.PIPE)
# DATABASE_URL = cprocess.stdout.decode("utf-8").strip("\n")

remote = 'dbname=atdatatest user=samcormack password={} host=localhost'.format(password)
local = 'dbname=atdatalocal user=samcormack password={} host=localhost'.format(password)
