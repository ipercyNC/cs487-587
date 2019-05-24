import mysql.connector
import random
import mysql_config

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
             1: {"count":1000,      "name":"base_onektup"},
             2: {"count":5000,      "name":"base_fivektup"},
             3: {"count":10000,     "name":"base_tenktup"},
             4: {"count":50000,     "name":"base_fiftyktup"},
             5: {"count":100000,    "name":"base_hundredktup"},
             6: {"count":500000,    "name":"base_fivehundredktup"},
             7: {"count":1000000,   "name":"base_onemtup"},
             8: {"count":5000000,   "name":"base_fivemtup"},
             9: {"count":100000000, "name":"base_tenmtup"}
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
        print filename
        cur.execute("select count(*) from "+str(filename))
        print cur.fetchall()
        return cur


def connect():
    try:
        db = mysql.connector.connect(user=mysql_config.config_param['user'], password=mysql_config.config_param['password'],
                                     host=mysql_config.config_param['host'],
                                     database=mysql_config.config_param['database'])
        cur = db.cursor()
        return cur, db
    except Exception as error:
        print(error)


def disconnect(cur, db):
    try:
        cur.execute("show tables")
        tables=cur.fetchall()
        for table in tables:
            print(table)
        db.close()
    except (Exception) as error:
            print(error)


def main():
    cur = connect()
    for i in range(1,7):
        rec = filenameIterate(i)
        # datagenfile(cur[0], rec['count'], rec['name'])
    disconnect(cur[0], cur[1])
    print("Disconnected")


if __name__ == "__main__":
    main()
