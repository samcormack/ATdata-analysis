import psycopg2 as pg
import psycopg2.extras as pgex
from psycopg2 import sql
import subprocess
from secret import password

class Copier():
    def __init__(self,localdsn,remotedsn):
        self.connfrom = pg.connect(remotedsn)
        self.connlocal = pg.connect(localdsn)
        self.curfrom = self.connfrom.cursor()
        self.curlocal = self.connlocal.cursor()

    def pulltable(self,table_name):
        # Copy new values from remote table to local
        self.curfrom.execute(sql.SQL("SELECT * FROM {};").format(sql.Identifier(table_name)))
        ncol = len(self.curfrom.description)
        q_put = sql.SQL("INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING;").format(
            sql.Identifier(table_name),
            sql.Placeholder()
        )
        pgex.execute_values(self.curlocal,q_put.as_string(self.connlocal),self.curfrom.fetchall())
        self.connlocal.commit()

    def cleartable(self,table_name):
        # Delete data from remote table. Continue incrementing primary key
        q_del = sql.SQL("TRUNCATE TABLE {} CONTINUE IDENTITY;").format(sql.Identifier(table_name))
        self.curfrom.execute(q_del)
        self.connfrom.commit()

    def close(self):
        # Close all cursors and connections
        self.curfrom.close()
        self.curlocal.close()
        self.connfrom.close()
        self.connlocal.close()

if __name__ == "__main__":

    # Heroku remote database
    cprocess = subprocess.run("heroku config:get DATABASE_URL -a serene-river-35528",
        stdout=subprocess.PIPE)
    remote = cprocess.stdout.decode("utf-8").strip("\n")

    # remote = 'dbname=atdatatest user=samcormack password={} host=localhost'.format(password)
    local = 'dbname=atdatalocal user=samcormack password={} host=localhost'.format(password)

    copier = Copier(local,remote)
    copier.pulltable('apipull_callrecord')
    copier.pulltable('apipull_route')
    copier.pulltable('apipull_stop')
    copier.pulltable('apipull_rtentry')
    copier.cleartable('apipull_rtentry')
    copier.close()
