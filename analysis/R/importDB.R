require(RPostgreSQL)
pw = ""


# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
con <- dbConnect(drv, dbname = "atdatalocal",
                 host = "localhost", port = 5432,
                 user = "samcormack", password = pw)
#rm(pw) # removes the password

q_get = "SELECT * FROM apipull_rtentry a INNER JOIN apipull_route r ON a.route_id_id=r.route_id INNER JOIN apipull_stop s ON a.stop_id_id=s.stop_id"
df = dbGetQuery(con,q_get)

# close the connection
dbDisconnect(con)
dbUnloadDriver(drv)