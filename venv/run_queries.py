

import psycopg2

from config import config

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

def cloneTables(cur, tempSchema, baseTable, clonedTables):
    """Clone tables from templates into tempSchema for running queries"""
    try:
        cur.execute("create schema if not exists "+tempSchema)
        for i in range(len(clonedTables)):
            if i == 2:
                cur.execute("create table if not exists " + tempSchema + "." + clonedTables[i] + " as table templates.onektup")
            else:
                cur.execute("create table if not exists "+tempSchema+"."+clonedTables[i]+" as table templates." +baseTable)
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def queries(cur, tempSchema, clonedTables):
    """ Connect to the PostgreSQL database server """
    try:
        times ={}
        cur.execute("create table "+tempSchema+".tmp as table "+tempSchema+"."+ clonedTables[0])
        cur.execute("create table "+tempSchema+".bprime as select * from "+tempSchema+"."+ clonedTables[1]+" where "+tempSchema+"."+clonedTables[1]+".unique2 <1000")
        #Query 13 from wisconsin (need to add cluster)
        cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) insert into "+tempSchema+".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                "twentyPercent,fiftyPercent,"
                                "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                "string4) select l.* from "+tempSchema+"."+clonedTables[0]+
                                " l,"+tempSchema+".bprime r where l.unique2 = r.unique2")
        print "runtime of query 13 from wisconsin"
        result = cur.fetchall()
        print result[0][0][0]["Execution Time"]
        cur.execute("delete from "+tempSchema+".tmp")
        cur.execute("select count(*) from "+tempSchema+".tmp")
        print "should have cleared tmp table out - results should have 0 count"
        print cur.fetchall()

        #Query 14 from wisconsin
        cur.execute(
            " EXPLAIN (ANALYZE, FORMAT JSON) insert into " +
            tempSchema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
            "twentyPercent,fiftyPercent,"
            "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
            "string4) select l.* from " + tempSchema + "." +clonedTables[2] +
            " l," +tempSchema+"."+clonedTables[0]+" r, "+tempSchema+"."+clonedTables[1]+" rr "
            "where l.unique2 = r.unique2 and r.unique2=rr.unique2 and r.unique2 <1000")
        print "runtime of query 14 from wisconsin"
        result = cur.fetchall()
        print result[0][0][0]["Execution Time"]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
def disconnect(cur, tempSchema):
    conn = None
    try:
        cur.execute("drop schema if exists "+ tempSchema+" cascade")
        # cur.execute("select schema_name from information_schema.schemata")
        # print cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



def main():
    tempSchema = "testing"
    baseTable= "tenktup"
    clonedTables=["tenktup1","tenktup2","onektup"]
    cur = connect(tempSchema)
    cloneTables(cur, tempSchema, baseTable, clonedTables)
    queries(cur, tempSchema, clonedTables)
    disconnect(cur, tempSchema)
    print("Disconnected")

if __name__ == "__main__":
    main()
