

import psycopg2

from config import config
import numpy as np

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
        allresults={}
        query13results=[]
        query14results=[]
        scaleupresults={}
        scaleupraw={}

        #Test Queries Part 2
        cur.execute("create table "+tempSchema+".tmp as table "+tempSchema+"."+ clonedTables[0])
        cur.execute("create table "+tempSchema+".bprime as select * from "+tempSchema+"."+ clonedTables[1]+" where "+tempSchema+"."+clonedTables[1]+".unique2 <1000")
        #Query 13 from wisconsin (need to add cluster)
        for i in range(10):
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) insert into "+tempSchema+".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                    "twentyPercent,fiftyPercent,"
                                    "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                    "string4) select l.* from "+tempSchema+"."+clonedTables[0]+
                                    " l,"+tempSchema+".bprime r where l.unique2 = r.unique2")
            result = cur.fetchall()
            query13results.append(result[0][0][0]["Execution Time"])
        query13results.remove(max(query13results))
        query13results.remove(min(query13results))
        # print ("Average of query 13", np.array(query13results).mean())
        # print query13results
        allresults['query13'] = np.array(query13results).mean().round(decimals=1)
        #Query 14 from wisconsin
        for i in range(10):
            cur.execute(
                " EXPLAIN (ANALYZE, FORMAT JSON) insert into " +
                tempSchema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                "twentyPercent,fiftyPercent,"
                "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                "string4) select l.* from " + tempSchema + "." +clonedTables[2] +
                " l," +tempSchema+"."+clonedTables[0]+" r, "+tempSchema+"."+clonedTables[1]+" rr "
                "where l.unique2 = r.unique2 and r.unique2=rr.unique2 and r.unique2 <1000")
            result = cur.fetchall()
            query14results.append(result[0][0][0]["Execution Time"])
        query14results.remove(max(query14results))
        query14results.remove(min(query14results))
        # print ("Average of query 14", np.array(query14results).mean())
        # print query14results
        allresults['query14'] = np.array(query14results).mean().round(decimals=1)

        #Test Queries Part 4

        scaleup = ['onektup','tenktup1']
        for i in range(len(scaleup)):
            scaleupraw[scaleup[i]] = []

            for j in range(10):
                cur.execute(
                    " EXPLAIN (ANALYZE, FORMAT JSON) select l.unique1, r.unique1, rr.unique1"
                    " from " + tempSchema + "." + scaleup[i] +
                    " l join " + tempSchema + "." + scaleup[i] + " r on l.unique2 = r.unique2 " +
                    "join " +tempSchema + "." + scaleup[i] + " rr  on l.unique2 = rr.unique2")
                result = cur.fetchall()
                scaleupraw[scaleup[i]].append(result[0][0][0]["Execution Time"])
            scaleupraw[scaleup[i]].remove(max(scaleupraw[scaleup[i]]))
            scaleupraw[scaleup[i]].remove(min(scaleupraw[scaleup[i]]))
            scaleupresults[scaleup[i]] = np.array(scaleupraw[scaleup[i]]).mean().round(decimals=1)
        print ("Scaleup test averages" ,scaleupresults)
        print ("All test Averages", allresults)
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
