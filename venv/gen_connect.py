
import random
import psycopg2
import time

from config import config

def cloneTables(cur, tempSchema):
    """Clone tables from templates into tempSchema for running queries"""
    try:
        cur.execute("create schema if not exists "+tempSchema)
        # print cur.fetchall()
        cur.execute("create table "+tempSchema+".tenktup1 as table templates.tenktup")
        cur.execute("create table "+tempSchema+".tenktup2 as table templates.tenktup")
        # cur.execute("select Table_Schema,Table_Name from information_schema.tables where table_type='BASE TABLE' and table_schema not like '%pg%' and table_schema not like '%information%'")
        # print cur.fetchall()
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
def connect(tempSchema):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("create schema if not exists "+tempSchema)
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def disconnect(cur, tempSchema):
    conn = None
    try:
        cur.execute("commit")
        cur.execute("select schema_name from information_schema.schemata")
        print cur.fetchall()
        # cur.execute("drop schema if exists "+tempSchema+" cascade")
        # cur.execute("select schema_name from information_schema.schemata")
        # print cur.fetchall()
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
    tableList = {
             1: {"count":1000,      "name":"onektup"},
             2: {"count":5000,      "name":"fivektup"},
             3: {"count":10000,     "name":"tenktup"},
             4: {"count":50000,     "name":"fiftyktup"},
             5: {"count":100000,    "name":"hundredktup"},
             6: {"count":500000,    "name":"fivehundredktup"},
             7: {"count":1000000,   "name":"onemtup"},
             8: {"count":5000000,   "name":"fivemtup"},
             9: {"count":100000000, "name":"tenmtup"}
    }
    # print tableList[index]
    return tableList[index]
def datagenfile(cur, count, filename):

        cur.execute("drop table if exists templates." +str(filename))
        cur.execute("CREATE TABLE templates."+str(filename)+"(" 
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
                cur.execute("insert into templates."+str(filename)+"(unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                                         "twentyPercent,fiftyPercent,"
                                                         "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                                         "string4"
                                                         ") values "+lineToInsert[:-1]+"")
                lineToInsert=''
        print filename
        cur.execute("select count(*) from templates."+str(filename))
        print cur.fetchall()
        return cur


def main():
    tempSchema = "templates"
    cur = connect(tempSchema)
    #Only need to run the generation once to the templates
    for i in range(1,7):
        rec = filenameIterate(i)
        datagenfile(cur, rec['count'], rec['name'])
    # cloneTables(cur, tempSchema)
    # queries(cur, "tenktup1", "tenktup2")
    disconnect(cur, tempSchema)
    print("Disconnected")

if __name__ == "__main__":
    main()
