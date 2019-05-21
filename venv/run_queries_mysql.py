import mysql.connector
import random
import mysql_config
import numpy as np


def connect():
    try:
        db = mysql.connector.connect(user=mysql_config.config_param['user'], password=mysql_config.config_param['password'],
                                     host=mysql_config.config_param['host'],
                                     database=mysql_config.config_param['database'])
        cur = db.cursor()
        return cur, db
    except Exception as error:
        print(error)



def cloneTables(cur, temp_schema, base_table, cloned_tables):
    """Clone tables from ipercy into tempSchema for running queries"""
    try:
        cur.execute("create schema if not exists " + temp_schema)
        for i in range(len(cloned_tables)):
            if i == 2:
                cur.execute("create table if not exists " + temp_schema + "." + cloned_tables[i] + " as select * from onektup")
            else:
                cur.execute("create table if not exists " + cloned_tables[i] + " as select * from " + base_table)
        return cur
    except Exception as error:
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
        partial_index_raw = {}
        partial_index_results = {}
        scaleup_raw = {}
        cur.execute("set profiling = 1")
        cur.execute("set profiling_history_size =100")
        # Test Queries Part 1
        # Can set to smaller table for testing but should be 100k or larger for results
        update_table_name = 'base_tenktup'
        # 25% selectivity
        update_raw['25% update'] = []
        cur_idx=0
        for j in range(10):
            cur.execute("create table if not exists " + temp_schema + ".tmp as select * from ipercy." + update_table_name)
            cur.execute(" update "+ temp_schema + ".tmp set two = 1 where four = 0 ")
            cur.execute("drop table " + temp_schema + ".tmp")
            cur.execute(" show profiles")
            result = cur.fetchall()
            print result[cur_idx+2]
            time = result[cur_idx+2][1]
            update_raw['25% update'].append(time)
            cur_idx = cur_idx+3
        update_raw['25% update'].remove(max(update_raw['25% update']))
        update_raw['25% update'].remove(min(update_raw['25% update']))
        update_results['25% update'] = np.array(update_raw['25% update']).mean().round(decimals=1)

        print update_raw
        print update_results

        update_raw['50% update'] = []
        for j in range(10):
            cur.execute("create table if not exists " + temp_schema + ".tmp as select * from ipercy." + update_table_name)
            cur.execute(" update "+ temp_schema + ".tmp set two = 1 where four <=1")
            cur.execute("drop table " + temp_schema + ".tmp")
            cur.execute(" show profiles")
            result = cur.fetchall()
            time = result[cur_idx+2][1]
            print result[cur_idx+2]
            update_raw['50% update'].append(time)
            cur_idx = (cur_idx+3) % 100
        update_raw['50% update'].remove(max(update_raw['50% update']))
        update_raw['50% update'].remove(min(update_raw['50% update']))
        update_results['50% update'] = np.array(update_raw['50% update']).mean().round(decimals=1)

        # print update_raw
        print update_results
        # # 50% selectivity
        # update_raw['50% update'] = []
        # for j in range(10):
        #     cur.execute("create table " + temp_schema + ".tmp as select * from ipercy." + update_table_name)
        #     cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=1 ")
        #     result = cur.fetchall()
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['50% update'].append(result[0][0][0]["Execution Time"])
        # update_raw['50% update'].remove(max(update_raw['50% update']))
        # update_raw['50% update'].remove(min(update_raw['50% update']))
        # update_results['50% update'] = np.array(update_raw['50% update']).mean().round(decimals=1)
        #
        # # 75% selectivity
        # update_raw['75% update'] = []
        # for j in range(10):
        #     cur.execute("create table " + temp_schema + ".tmp as table ipercy." + update_table_name)
        #     cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=2 ")
        #     result = cur.fetchall()
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['75% update'].append(result[0][0][0]["Execution Time"])
        # update_raw['75% update'].remove(max(update_raw['75% update']))
        # update_raw['75% update'].remove(min(update_raw['75% update']))
        # update_results['75% update'] = np.array(update_raw['75% update']).mean().round(decimals=1)
        #
        # # 100% selectivity
        # update_raw['100% update'] = []
        # for j in range(10):
        #     cur.execute("create table " + temp_schema + ".tmp as table ipercy." + update_table_name)
        #     cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) update "+ temp_schema + ".tmp set two = 1 where four <=3 ")
        #     result = cur.fetchall()
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['100% update'].append(result[0][0][0]["Execution Time"])
        # update_raw['100% update'].remove(max(update_raw['100% update']))
        # update_raw['100% update'].remove(min(update_raw['100% update']))
        # update_results['100% update'] = np.array(update_raw['100% update']).mean().round(decimals=1)
        #
        # # Test Queries Part 2
        # cur.execute("create table " + temp_schema + ".tmp as table " + temp_schema + "." + cloned_tables[0])
        # cur.execute("create table " + temp_schema + ".bprime as select * from " + temp_schema + "." + cloned_tables[1] + " where " + temp_schema + "." + cloned_tables[1] + ".unique2 <1000")
        # # Query 13 from wisconsin (need to add cluster)
        # for i in range(10):
        #     cur.execute(" EXPLAIN (ANALYZE, FORMAT JSON) insert into " + temp_schema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
        #                             "twentyPercent,fiftyPercent,"
        #                             "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
        #                             "string4) select l.* from " + temp_schema + "." + cloned_tables[0] +
        #                             " l," + temp_schema + ".bprime r where l.unique2 = r.unique2")
        #     result = cur.fetchall()
        #     query13_results.append(result[0][0][0]["Execution Time"])
        # query13_results.remove(max(query13_results))
        # query13_results.remove(min(query13_results))
        # all_results['query13'] = np.array(query13_results).mean().round(decimals=1)
        #
        # # Query 14 from wisconsin
        # for i in range(10):
        #     cur.execute(
        #         " EXPLAIN (ANALYZE, FORMAT JSON) insert into " +
        #         temp_schema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
        #         "twentyPercent,fiftyPercent,"
        #         "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
        #         "string4) select l.* from " + temp_schema + "." + cloned_tables[2] +
        #         " l," + temp_schema + "." + cloned_tables[0] + " r, " + temp_schema + "." + cloned_tables[1] + " rr "
        #         "where l.unique2 = r.unique2 and r.unique2=rr.unique2 and r.unique2 <1000")
        #     result = cur.fetchall()
        #     query14_results.append(result[0][0][0]["Execution Time"])
        # query14_results.remove(max(query14_results))
        # query14_results.remove(min(query14_results))
        # all_results['query14'] = np.array(query14_results).mean().round(decimals=1)
        #
        # # Test Queries Part 3
        # # Values for selection with partial index
        # partial_index_raw['partial'] = []
        # # Values for selection without partial index
        # partial_index_raw['non-partial'] = []
        #
        # for i in range(10):
        #     cur.execute("drop table if exists " + temp_schema +".tmp")
        #     cur.execute("create table " + temp_schema + ".tmp as table ipercy." + update_table_name)
        #     cur.execute("create index idx_two_value_0 on " + temp_schema + ".tmp(two) where two=0")
        #     cur.execute("explain (analyze, format json) select l.* from " + temp_schema + ".tmp l join "+temp_schema+".tmp r on l.two=r.two where l.two =0")
        #     result = cur.fetchall()
        #     partial_index_raw['partial'].append(result[0][0][0]["Execution Time"])
        #     cur.execute("explain (analyze, format json) select l.* from " + temp_schema + ".tmp l join "+temp_schema+".tmp r on l.two=r.two where l.two =1")
        #     result = cur.fetchall()
        #     partial_index_raw['non-partial'].append(result[0][0][0]["Execution Time"])
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #
        # partial_index_raw['partial'].remove(max(partial_index_raw['partial']))
        # partial_index_raw['partial'].remove(max(partial_index_raw['partial']))
        # partial_index_results['partial'] = np.array(partial_index_raw['partial']).mean().round(decimals=1)
        # partial_index_raw['non-partial'].remove(max(partial_index_raw['non-partial']))
        # partial_index_raw['non-partial'].remove(max(partial_index_raw['non-partial']))
        # partial_index_results['non-partial'] = np.array(partial_index_raw['non-partial']).mean().round(decimals=1)
        # print partial_index_results
        # # Test Queries Part 4
        # scaleup = ['onektup', 'tenktup', 'hundredktup', 'fivehundredktup']
        # for i in range(len(scaleup)):
        #     scaleup_raw[scaleup[i]] = []
        #
        #     for j in range(10):
        #         cur.execute(
        #             " EXPLAIN (ANALYZE, FORMAT JSON) select l.unique1, r.unique1, rr.unique1"
        #             " from ipercy." + scaleup[i] +
        #             " l join ipercy." + scaleup[i] + " r on l.unique2 = r.unique2 " +
        #             "join ipercy." + scaleup[i] + " rr  on l.unique2 = rr.unique2")
        #         result = cur.fetchall()
        #         scaleup_raw[scaleup[i]].append(result[0][0][0]["Execution Time"])
        #     scaleup_raw[scaleup[i]].remove(max(scaleup_raw[scaleup[i]]))
        #     scaleup_raw[scaleup[i]].remove(min(scaleup_raw[scaleup[i]]))
        #     scaleup_results[scaleup[i]] = np.array(scaleup_raw[scaleup[i]]).mean().round(decimals=1)
        #
        # print ("Scaleup test averages", scaleup_results)
        # print ("Update averages", update_results)
        # print ("All test Averages", all_results)

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
    temp_schema = 'ipercy'
    base_table = "base_tenktup"
    cloned_tables = ["tenktup1"]
    cur = connect()
    cloneTables(cur[0], temp_schema, base_table, cloned_tables)
    queries(cur[0], temp_schema, cloned_tables)
    disconnect(cur[0], cur[1])
    print("Disconnected")



if __name__ == "__main__":
    main()
