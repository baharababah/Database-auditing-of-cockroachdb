import pyspark
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder.appName("SonarData").getOrCreate()

# Read the Sonar file
dataframe = spark.read.csv("D:\DataScience/CockroachDB/Task/Sonar.csv", \
                                          header = True,
                                           sep=",")
# show dataframe
dataframe.show(3)


# cockroach start-single-node --insecure  --listen-addr=localhost:26257 --http-addr=localhost:8080
# connect to the DB
t_dbname = "postgres"
t_name_user = "root"
t_sslmode = "disable"
t_sslrootcert = 'mycerts/ca.crt'
t_sslkey = 'mycerts/client.maxroach.key'
t_sslcert = 'mycerts/client.maxroach.crt'
t_host = "localhost"
t_port = "26257"
# Note the connection string for CockroachDB differs slightly from the one for PostgreSQL.
data_connection = psycopg2.connect(database=t_dbname, user=t_name_user, sslmode=t_sslmode, sslrootcert=t_sslrootcert, sslkey=t_sslkey, sslcert=t_sslcert, host=t_host, port=t_port)
db_cursor = data_connection.cursor()


db_cursor.execute("USE postgres;")
db_cursor.execute('CREATE TABLE sonar (V1 REAL,\
                                      V2 REAL,\
                                      V3 REAL,\
                                      V4 REAL,\
                                      V5 REAL,\
                                      V6 REAL,\
                                      V7 REAL,\
                                      V8 REAL,\
                                      V9 REAL,\
                                      V10 REAL,\
                                      V11 REAL,\
                                      V12 REAL,\
                                      V13 REAL,\
                                      V14 REAL,\
                                      V15 REAL,\
                                      V16 REAL,\
                                      V17 REAL,\
                                      V18 REAL,\
                                      V19 REAL,\
                                      V20 REAL,\
                                      V21 REAL,\
                                      V22 REAL,\
                                      V23 REAL,\
                                      V24 REAL,\
                                      V25 REAL,\
                                      V26 REAL,\
                                      V27 REAL,\
                                      V28 REAL,\
                                      V29 REAL,\
                                      V30 REAL,\
                                      V31 REAL,\
                                      V32 REAL,\
                                      V33 REAL,\
                                      V34 REAL,\
                                      V35 REAL,\
                                      V36 REAL,\
                                      V37 REAL,\
                                      V38 REAL,\
                                      V39 REAL,\
                                      V40 REAL,\
                                      V41 REAL,\
                                      V42 REAL,\
                                      V43 REAL,\
                                      V44 REAL,\
                                      V45 REAL,\
                                      V46 REAL,\
                                      V47 REAL,\
                                      V48 REAL,\
                                      V49 REAL,\
                                      V50 REAL,\
                                      V51 REAL,\
                                      V52 REAL,\
                                      V53 REAL,\
                                      V54 REAL,\
                                      V55 REAL,\
                                      V56 REAL,\
                                      V57 REAL,\
                                      V58 REAL,\
                                      V59 REAL,\
                                      V60 REAL,\
                                      Class INT\
                                             );')

# turn on auditing for Sonar table
db_cursor.execute("ALTER TABLE sonar EXPERIMENTAL_AUDIT SET READ WRITE;")


# inser the dataframe content to Sonar table
for x in dataframe.collect():
    q = "INSERT INTO sonar (V1, V2,	V3,	V4,	V5,	V6,	V7,	V8,	V9,	V10,\
                            V11, V12, V13, V14,	V15, V16, V17, V18,	V19, V20,\
                            V21, V22, V23, V24,	V25, V26, V27,	V28, V29, V30,\
                            V31, V32, V33, V34,	V35, V36, V37, V38,	V39, V40,\
                            V41, V42, V43, V44,	V45, V46, V47, V48,	V49, V50,\
                            V51, V52, V53, V54,	V55, V56, V57,	V58, V59, V60, Class) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                                    %s)"
    a = tuple(x)
    db_cursor.execute(q, a)
data_connection.commit()

# select
db_cursor.execute("SELECT * FROM sonar;")
result = db_cursor.fetchmany(5)
for row in result:
    print(row)
