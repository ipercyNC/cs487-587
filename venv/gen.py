
import csv
import random
import sys
import psycopg2
from config import config
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # execute a statement
        # cur.execute("CREATE TABLE onektup("
        #             "unique1 integer,"
        #             "unique2 integer,"
        #             "two integer,"
        #             "four integer,"
        #             "twenty integer,"
        #             "onePercent integer,"
        #             "tenPercent integer,"
        #             "twentyPercent integer,"
        #             "fiftyPercent integer,"
        #             "unique3 integer,"
        #             "evenOnePercent integer,"
        #             "oddOnePercent integer,"
        #             "stringu1 VARCHAR(52),"
        #             "stringu2 VARCHAR(52),"
        #             "string4 VARCHAR(52),"
        #             "UNIQUE(unique1,unique2,unique3)"
        #             ")")
        # close the communication with the PostgreSQL
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def disconnect(cur):
    conn = None
    try:
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print cur.fetchall()
        cur.execute("drop table if exists onektup")
        cur.execute("drop table if exists tenktup2")
        cur.execute("drop table if exists tmp")
        cur.execute("drop table if exists tenktup")
        cur.execute("drop table if exists tenktup1")
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print cur.fetchall()
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



def convert(unique):
    result = list("AAAAAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    temp = list("AAAAAAA")
    i = 6
    cnt = 0
    while unique > 0:
        rem = unique % 26
        temp[i]=chr(ord('A')+rem)
        unique = int(unique/26)
        i = i-1
        cnt = cnt +1
    j = 0
    for x in range(i+1,7):
        result[j] = temp[x]
        j = j + 1
    return "".join(result)

def str4Select(index):
    switcher = {
        0: "AAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        1: "HHHHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        2: "OOOOxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        3: "VVVVxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        }
    return switcher.get(index%4)

def filenameSelect(count):
    switcher = {
        1000: "onektup.csv",
        5000: "fivektup.csv",
        10000: "tenktup.csv",
        50000: "fiftyktup.csv",
        100000: "hundredktup.csv",
        500000: "fivehudredktup.csv",
        1000000: "onemtup.csv",
        5000000: "fivemtup.csv",
        10000000: "tenmtup.csv",
        }
    return switcher.get(count)

def datagenfile(cur, count, filename):
        cur.execute("drop table if exists onektup")
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
            cur.execute("insert into onektup(unique1) values("+str(unique1)+")")
            cur.execute("select * from onektup")
            print cur.fetchall()
                        # "[unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4])


def main(argv):
    count = int(sys.argv[1])
    cur = connect()
    filename = filenameSelect(count)
    print(count, filename)
    datagenfile(cur, count, filename)
    disconnect(cur)

if __name__ == "__main__":
    main(sys.argv[1:])
