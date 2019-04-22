#!/usr/bin/python
import psycopg2
from config import config

def datagenline(count, filename):
        tupleCount = count
        numList = random.sample(range(0,tupleCount),tupleCount)
        for i in range(tupleCount):
            unique1 = numList[i]
            unique2 = i
            two= unique1 % 2
            four = unique1 % 4
            ten = unique1 % 10
            twenty = unique1 % 20
            onePercent = unique1 % 100
            tenPercent = unique1 % 10
            twentyPercent = unique1 % 5
            fiftyPercent = unique1 % 2
            unique3 = unique1
            evenOnePercent = onePercent *2
            oddOnePercent = onePercent * 2 + 1
            stringu1 = convert(unique1)
            stringu2 = convert(unique2)
            string4 = str4Select(i)

[unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4])


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        gentableline(cur)
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()

        print(db_version)
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print cur.fetchall()

        cur.execute("drop table if exists onektup")
        cur.execute("drop table if exists tenktup2")
        cur.execute("drop table if exists tmp")
        cur.execute("drop table if exists tenktup")
        cur.execute("drop table if exists tenktup1")
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print cur.fetchall()
        cur.execute("CREATE TABLE onektup(" 
                    "unique1 integer,"
                    "unique2 integer,"
                    "two integer,"
                    "four integer,"
                    "twenty integer,"
                    "onePercent integer,"
                    "tenPercent integer,"
                    "twentyPercent integer,"
                    "fiftyPercent integer,"
                    "unique3 integer,"
                    "evenOnePercent integer,"
                    "oddOnePercent integer,"
                    "stringu1 VARCHAR(52),"
                    "stringu2 VARCHAR(52),"
                    "string4 VARCHAR(52),"
                    "UNIQUE(unique1,unique2,unique3)"
                    ")")
        cur.execute("select * from onektup")
        print cur.fetchall()
        cur.execute("insert into onektup(unique1) values(44)")
        cur.execute("select * from onektup")
        print cur.fetchall()

        #cur.execute("COPY onektup(unique1,unique2,two,four,twenty,onePercent,tenPercent,twentyPercent,fiftyPercent,unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,string4)"
         #           "from 'C:\ipercy\Desktop\databases\project\cs487-587\venv\onektup.csv' DELIMITER ',' CSV HEADER")
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print cur.fetchall()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()