import psycopg2
from config import config
import numpy as np


def connect(temp_schema):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("create schema if not exists " + temp_schema)
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def cloneTables(cur, temp_schema, base_table, cloned_tables):
    """Clone tables from templates into tempSchema for running queries"""
    try:
        cur.execute("create schema if not exists " + temp_schema)
        for i in range(len(cloned_tables)):
            if i == 2:
                cur.execute("create table if not exists " + temp_schema + "." + cloned_tables[i] + " as table templates.onektup")
            else:
                cur.execute("create table if not exists " + temp_schema + "." + cloned_tables[i] + " as table templates." + base_table)
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def queries(cur, temp_schema, cloned_tables):
    """ Connect to the PostgreSQL database server """
    try:
        all_results={}
        update_results = {}
        update_raw = {}
        query13_results = []
        query14_results = []
        scaleup_results = {}
        scaleup_raw = {}

        # Test Queries Part 1
        # Can set to smaller table for testing but should be 100k or larger for results
        update_table_name = 'hundredktup'
        # 25% selectivity
        update_raw['25% update'] = []
        for j in range(10):
            cur.execute("create table " + temp_schema + ".tmp as table templates." + update_table_name)
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four = 0 ")
            result = cur.fetchall()
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['25% update'].append(result[0][0][0]["Execution Time"])
        update_raw['25% update'].remove(max(update_raw['25% update']))
        update_raw['25% update'].remove(min(update_raw['25% update']))
        update_results['25% update'] = np.array(update_raw['25% update']).mean().round(decimals=1)

        # 50% selectivity
        update_raw['50% update'] = []
        for j in range(10):
            cur.execute("create table " + temp_schema + ".tmp as table templates." + update_table_name)
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=1 ")
            result = cur.fetchall()
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['50% update'].append(result[0][0][0]["Execution Time"])
        update_raw['50% update'].remove(max(update_raw['50% update']))
        update_raw['50% update'].remove(min(update_raw['50% update']))
        update_results['50% update'] = np.array(update_raw['50% update']).mean().round(decimals=1)

        # 75% selectivity
        update_raw['75% update'] = []
        for j in range(10):
            cur.execute("create table " + temp_schema + ".tmp as table templates." + update_table_name)
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=2 ")
            result = cur.fetchall()
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['75% update'].append(result[0][0][0]["Execution Time"])
        update_raw['75% update'].remove(max(update_raw['75% update']))
        update_raw['75% update'].remove(min(update_raw['75% update']))
        update_results['75% update'] = np.array(update_raw['75% update']).mean().round(decimals=1)

        # 100% selectivity
        update_raw['100% update'] = []
        for j in range(10):
            cur.execute("create table " + temp_schema + ".tmp as table templates." + update_table_name)
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=3 ")
            result = cur.fetchall()
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['100% update'].append(result[0][0][0]["Execution Time"])
        update_raw['100% update'].remove(max(update_raw['100% update']))
        update_raw['100% update'].remove(min(update_raw['100% update']))
        update_results['100% update'] = np.array(update_raw['100% update']).mean().round(decimals=1)

        # Test Queries Part 2
        cur.execute("create table " + temp_schema + ".tmp as table " + temp_schema + "." + cloned_tables[0])
        cur.execute("create table " + temp_schema + ".bprime as select * from " + temp_schema + "." + cloned_tables[1] + " where " + temp_schema + "." + cloned_tables[1] + ".unique2 <1000")
        # Query 13 from wisconsin (need to add cluster)
        for i in range(10):
            cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) insert into " + temp_schema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                    "twentyPercent,fiftyPercent,"
                                    "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                    "string4) select l.* from " + temp_schema + "." + cloned_tables[0] +
                                    " l," + temp_schema + ".bprime r where l.unique2 = r.unique2")
            result = cur.fetchall()
            query13_results.append(result[0][0][0]["Execution Time"])
        query13_results.remove(max(query13_results))
        query13_results.remove(min(query13_results))
        all_results['query13'] = np.array(query13_results).mean().round(decimals=1)

        # Query 14 from wisconsin
        for i in range(10):
            cur.execute(
                " EXPLAIN (ANALYZE, FORMAT JSON) insert into " +
                temp_schema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                "twentyPercent,fiftyPercent,"
                "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                "string4) select l.* from " + temp_schema + "." + cloned_tables[2] +
                " l," + temp_schema + "." + cloned_tables[0] + " r, " + temp_schema + "." + cloned_tables[1] + " rr "
                "where l.unique2 = r.unique2 and r.unique2=rr.unique2 and r.unique2 <1000")
            result = cur.fetchall()
            query14_results.append(result[0][0][0]["Execution Time"])
        query14_results.remove(max(query14_results))
        query14_results.remove(min(query14_results))
        all_results['query14'] = np.array(query14_results).mean().round(decimals=1)

        # Test Queries Part 4
        scaleup = ['onektup', 'tenktup', 'hundredktup', 'fivehundredktup']
        for i in range(len(scaleup)):
            scaleup_raw[scaleup[i]] = []

            for j in range(10):
                cur.execute(
                    " EXPLAIN (ANALYZE, FORMAT JSON) select l.unique1, r.unique1, rr.unique1"
                    " from templates." + scaleup[i] +
                    " l join templates." + scaleup[i] + " r on l.unique2 = r.unique2 " +
                    "join templates." + scaleup[i] + " rr  on l.unique2 = rr.unique2")
                result = cur.fetchall()
                scaleup_raw[scaleup[i]].append(result[0][0][0]["Execution Time"])
            scaleup_raw[scaleup[i]].remove(max(scaleup_raw[scaleup[i]]))
            scaleup_raw[scaleup[i]].remove(min(scaleup_raw[scaleup[i]]))
            scaleup_results[scaleup[i]] = np.array(scaleup_raw[scaleup[i]]).mean().round(decimals=1)

        print ("Scaleup test averages", scaleup_results)
        print ("Update averages", update_results)
        print ("All test Averages", all_results)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def disconnect(cur, temp_schema):
    conn = None
    try:
        cur.execute("drop schema if exists " + temp_schema+" cascade")
        cur.execute("commit")
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
    temp_schema = "testing"
    base_table = "tenktup"
    cloned_tables = ["tenktup1", "tenktup2", "onektup"]
    cur = connect(temp_schema)
    cloneTables(cur, temp_schema, base_table, cloned_tables)
    queries(cur, temp_schema, cloned_tables)
    disconnect(cur, temp_schema)
    print("Disconnected")


if __name__ == "__main__":
    main()
