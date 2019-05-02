
import random
import psycopg2
import time

from config import config
def queries(cur, count, tableName):
    """ Connect to the PostgreSQL database server """
    try:
        onePercent = count * 0.01;
        tenPercent = count * 0.10;
        print tableName +" "+ str(onePercent)
        start = time.time()
        cur.execute("select * from "+tableName+" where unique2 between 0 and " +str(onePercent))
        end = time.time()

        print "runtime of first query"
        print (end-start)
        cur.execute("select unique2 from onektup into temp table bprime;")
        print cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
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
        cur.execute("set enable_hashjoin=false")
        cur.execute("show all")
        print cur.fetchall()
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def disconnect(cur):
    conn = None
    try:
        # cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        # print cur.fetchall()
        cur.execute("drop table if exists onektup")
        cur.execute("drop table if exists fivektup")
        cur.execute("drop table if exists tenktup")
        cur.execute("drop table if exists tenktup1")
        cur.execute("drop table if exists tenktup2")
        cur.execute("drop table if exists fiftyktup")
        cur.execute("drop table if exists hundredktup")
        cur.execute("drop table if exists fivehundredktup")
        cur.execute("drop table if exists onemtup")
        cur.execute("drop table if exists fivemtup")
        cur.execute("drop table if exists tenmtup")
        cur.execute("drop table if exists tmp")

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
             2: {"count":5000,     "name":"fivektup"},
             3: {"count":10000,    "name":"tenktup1"},
             4: {"count":10000,    "name":"tenktup2"},
             5: {"count":50000,    "name":"fiftyktup"},
             6: {"count":100000,   "name":"hundredktup"},
             7: {"count":500000,   "name": "fivehundredktup"},
             8: {"count":1000000,  "name":"onemtup"},
             9: {"count":5000000,  "name":"fivemtup"},
            10: {"count":100000000,"name":"tenmtup"}
    }
    # print tableList[index]
    return tableList[index]
def datagenfile(cur, count, filename):

        cur.execute("drop table if exists " +str(filename))
        cur.execute("CREATE TABLE "+str(filename)+"(" 
                    "unique1 integer,"
                    "unique2 integer,"
                    "two integer,"
                    "four integer,"
                    "ten integer,"                               
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
        if filename[:-4]=='mtup' or filename == 'fivehundredktup':
            packetSize = 10000
        else:
            packetSize = 1000
        tupleCount = count
        numList = random.sample(range(0,tupleCount),tupleCount)
        lineToInsert =''
        for i in range(0,tupleCount):
            # if (i!=0 and (i)%1000==0):
            #     lineToInsert+=","
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
            lineToInsert += "(" +str(unique1)+","+str(unique2)+","+str(two)+","+str(four) +","
            lineToInsert += str(ten)+","+str(twenty)+","+str(onePercent)+"," +str(tenPercent) +","
            lineToInsert += str(twentyPercent)+","+str(fiftyPercent)+","+str(unique3)+","
            lineToInsert += str(evenOnePercent)+","+str(oddOnePercent)+",'"+stringu1+"','"+stringu2+"','"+string4+"'),"
            # print lineToInsert
            if (i+1) % packetSize ==0:
                cur.execute("insert into "+str(filename)+"(unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                                         "twentyPercent,fiftyPercent,"
                                                         "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                                         "string4"
                                                         ") values "+lineToInsert[:-1]+"")
                lineToInsert=''

        # print("for table",str(filename), "line count")
        cur.execute("select count(*) from "+str(filename))
        # print cur.fetchall()
        # print cur.fetchone()


def main():
    cur = connect()
    # for i in range(1,5):
    #     rec = filenameIterate(i)
    #     datagenfile(cur, rec['count'], rec['name'])
    #     queries(cur, rec['count'], rec['name'])
    disconnect(cur)
    print("Disconnected")

if __name__ == "__main__":
    main()
