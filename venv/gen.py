
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
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def disconnect(cur):
    conn = None
    try:
        # cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        # print cur.fetchall()
        cur.execute("drop table if exists onektup")
        cur.execute("drop table if exists tenktup2")
        cur.execute("drop table if exists tmp")
        cur.execute("drop table if exists tenktup")
        cur.execute("drop table if exists tenktup1")
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

def filenameIterate(index):
    tableList = {1: {"count":1000, "name":"onektup"},
             2: {"count":5000, "name":"fivektup"},
             3: {"count":10000, "name":"tenktup1"},
             4: {"count":10000, "name":"tenktup2"}
    }
    print tableList[index]
    return tableList[index]
def datagenfile(cur, count, filename):
        cur.execute("drop table if exists " +str(filename))
        cur.execute("CREATE TABLE "+str(filename)+"(" 
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
        lineToInsert =''
        for i in range(1,tupleCount):
            if (i!=1 and (i-1)%100!=0):
                lineToInsert+=","
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
            lineToInsert += "(" +str(unique1)+","+str(unique2)+","+str(two)+","+str(four) +")"
            # print lineToInsert
            if i % 100 ==0:
                cur.execute("insert into "+str(filename)+"(unique1,unique2,two,four) values "+lineToInsert+"")
                lineToInsert=''
        cur.execute("select * from "+str(filename))
        print cur.fetchone()


def main():
    # count = int(sys.argv[1])
    cur = connect()
    for i in range(1,4):
        rec = filenameIterate(i)
        print(rec['count'], rec['name'])
        datagenfile(cur, rec['count'], rec['name'])
    disconnect(cur)
    print("Disconnected")

if __name__ == "__main__":
    main()
