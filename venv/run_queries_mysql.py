import mysql.connector
import random
import mysql_config
import numpy as np


def connect():
    try:
        db = mysql.connector.connect(user=mysql_config.config_param['user'], password=mysql_config.config_param['password'],
                                     host=mysql_config.config_param['host'],
                                     database=mysql_config.config_param['database'])
        cur = db.cursor(buffered=True)
        return cur, db
    except Exception as error:
        print(error)



def cloneTables(cur, temp_schema, base_table, cloned_tables):
    """Clone tables from ipercy into tempSchema for running queries"""
    try:
        cur.execute("create schema if not exists " + temp_schema)
        for i in range(len(cloned_tables)):
            if i == 2:
                cur.execute("create table if not exists " + temp_schema + "." + cloned_tables[i] + " as select * from golden_onektup")
            else:
                cur.execute("create table if not exists " + cloned_tables[i] + " as select * from " + base_table)
        return cur
    except Exception as error:
        print(error)


def queries(cur, temp_schema, base_table, cloned_tables):
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
        cur.execute("set profiling_history_size =1")
        update_table_name=base_table
        # Test Queries Part 1
        # Can set to smaller table for testing but should be 100k or larger for results
        #25% selectivity
        # update_raw['25% update'] = []
        # for j in range(10):
        #     cur.execute("create table if not exists " + temp_schema + ".tmp as select * from "+temp_schema+"." + update_table_name)
        #     cur.execute(" update " + temp_schema + ".tmp set two = 1 where four = 0 ")
        #     cur.execute(" show profiles")
        #     result = cur.fetchall()
        #     # print result[0]
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     time = result[0][1]
        #     update_raw['25% update'].append(time)
        # update_raw['25% update'].remove(max(update_raw['25% update']))
        # update_raw['25% update'].remove(min(update_raw['25% update']))
        # all_results['test1-25%-update'] = np.array(update_raw['25% update']).mean().round(decimals=3)
        # print all_results
        #
        # update_raw['50% update'] = []
        # for j in range(10):
        #     cur.execute("create table if not exists " + temp_schema + ".tmp as select * from "+temp_schema+ "." + update_table_name)
        #     cur.execute(" update "+ temp_schema + ".tmp set two = 1 where four <=1")
        #     cur.execute(" show profiles")
        #     result = cur.fetchall()
        #     time = result[0][1]
        #     # print result[0]
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['50% update'].append(time)
        # update_raw['50% update'].remove(max(update_raw['50% update']))
        # update_raw['50% update'].remove(min(update_raw['50% update']))
        # all_results['test1-50%-update'] = np.array(update_raw['50% update']).mean().round(decimals=3)
        # print all_results
        #
        # update_raw['75% update'] = []
        # for j in range(10):
        #     cur.execute("create table if not exists " + temp_schema + ".tmp as select * from "+temp_schema+"." + update_table_name)
        #     cur.execute(" update "+ temp_schema + ".tmp set two = 1 where four <=2")
        #     cur.execute(" show profiles")
        #     result = cur.fetchall()
        #     time = result[0][1]
        #     # print result[0]
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['75% update'].append(time)
        # update_raw['75% update'].remove(max(update_raw['75% update']))
        # update_raw['75% update'].remove(min(update_raw['75% update']))
        # all_results['test1-75%-update'] = np.array(update_raw['75% update']).mean().round(decimals=3)
        # print all_results
        #
        # update_raw['100% update'] = []
        # for j in range(10):
        #     cur.execute("create table if not exists " + temp_schema + ".tmp as select * from "+temp_schema+ "." + update_table_name)
        #     cur.execute(" update "+ temp_schema + ".tmp set two = 1 where four <=3")
        #     cur.execute(" show profiles")
        #     result = cur.fetchall()
        #     time = result[0][1]
        #     # print result[0]
        #     cur.execute("drop table " + temp_schema + ".tmp")
        #     update_raw['100% update'].append(time)
        # update_raw['100% update'].remove(max(update_raw['100% update']))
        # update_raw['100% update'].remove(min(update_raw['100% update']))
        # all_results['test1-100%-update'] = np.array(update_raw['100% update']).mean().round(decimals=3)
        # print all_results

        # Bulk update after a join - 2 tables same size
        update_raw['After Join'] = []
        for j in range(10):
            cur.execute("create table if not exists " + temp_schema + ".tmp as select * from "+temp_schema+ "." + update_table_name)
            cur.execute("update "+temp_schema+".tmp l join "+temp_schema+".tmp r on l.four = r.four set l.two=1 where r.four =0")
            cur.execute(" show profiles")
            result = cur.fetchall()
            time = result[0][1]
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['After Join'].append(time)
        update_raw['After Join'].remove(max(update_raw['After Join']))
        update_raw['After Join'].remove(min(update_raw['After Join']))
        all_results['test1-after-join'] = np.array(update_raw['After Join']).mean().round(decimals=1)
        print all_results

        # Bulk update on an index
        update_raw['Update Index'] = []
        for j in range(10):
            cur.execute("create table if not exists " + temp_schema + ".tmp as select * from " + temp_schema + "." + update_table_name)
            cur.execute("update "+temp_schema +".tmp set unique2=unique1")
            cur.execute(" show profiles")
            result = cur.fetchall()
            time = result[0][1]
            cur.execute("drop table " + temp_schema + ".tmp")
            update_raw['Update Index'].append(time)
        update_raw['Update Index'].remove(max(update_raw['Update Index']))
        update_raw['Update Index'].remove(min(update_raw['Update Index']))
        all_results['test1-update-index'] = np.array(update_raw['Update Index']).mean().round(decimals=1)
        print all_results

        # Test Queries Part 2
        cur.execute("drop table if exists " + temp_schema+ ".tmp")
        cur.execute("drop table if exists " + temp_schema+ ".bprime")
        cur.execute("create table " + temp_schema + ".tmp as select * from "+ base_table)
        cur.execute("create table " + temp_schema + ".bprime as select * from " + base_table + " where "+base_table + ".unique2 <1000")

        # Query 13 from wisconsin (need to add cluster)
        for i in range(10):
            cur.execute(" insert into tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                    "twentyPercent,fiftyPercent,"
                                    "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
                                    "string4) select l.* from " + cloned_tables[0] +
                                    " l, bprime r where l.unique2 = r.unique2")
            cur.execute("show profiles")
            result = cur.fetchall()
            time = result[0][1]
            query13_results.append(time)
        query13_results.remove(max(query13_results))
        query13_results.remove(min(query13_results))
        all_results['test2-query13'] = np.array(query13_results).mean().round(decimals=3)
        print all_results

        # # Query 14 from wisconsin
        # for i in range(10):
        #     cur.execute(
        #         "insert into " +
        #         temp_schema + ".tmp (unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
        #         "twentyPercent,fiftyPercent,"
        #         "unique3,evenOnePercent,oddOnePercent,stringu1,stringu2,"
        #         "string4) select l.* from " + temp_schema + "." + cloned_tables[2] +
        #         " l," + temp_schema + "." + cloned_tables[0] + " r, " + temp_schema + "." + cloned_tables[1] + " rr "
        #         "where l.unique2 = r.unique2 and r.unique2=rr.unique2 and r.unique2 <1000")
        #     cur.execute("show profiles")
        #     result = cur.fetchall()
        #     # print result[0]
        #     time = result[0][1]
        #     query14_results.append(time)
        # query14_results.remove(max(query14_results))
        # query14_results.remove(min(query14_results))
        # all_results['test2-query14'] = np.array(query14_results).mean().round(decimals=3)
        # print all_results

        # Test Queries Part 3
        # Values for selection with partial index
        partial_index_raw['partial'] = []
        # Values for selection without partial index
        partial_index_raw['non-partial'] = []

        for i in range(10):
            cur.execute("drop table if exists " + temp_schema +".tmp")
            cur.execute("create table " + temp_schema + ".tmp as select * from "+temp_schema + "." + update_table_name)
            # cur.execute("create index idx_two_value_0 on " + temp_schema + ".tmp(two) where two=0")
            cur.execute("select * from " +temp_schema +".tmp where ten =1")
            cur.execute("show profiles")
            result = cur.fetchall()
            time = result[0][1]
            partial_index_raw['partial'].append(time)
            cur.execute("select * from " + temp_schema + ".tmp where ten =0")
            cur.execute("show profiles")
            result = cur.fetchall()
            time = result[0][1]
            partial_index_raw['non-partial'].append(time)
            cur.execute("drop table " + temp_schema + ".tmp")
        partial_index_raw['partial'].remove(max(partial_index_raw['partial']))
        partial_index_raw['partial'].remove(max(partial_index_raw['partial']))
        all_results['test3-partial'] = np.array(partial_index_raw['partial']).mean().round(decimals=3)
        partial_index_raw['non-partial'].remove(max(partial_index_raw['non-partial']))
        partial_index_raw['non-partial'].remove(max(partial_index_raw['non-partial']))
        all_results['test3-non-partial'] = np.array(partial_index_raw['non-partial']).mean().round(decimals=3)
        print all_results

        # Test Queries Part 4
        scaleup = ['golden_onektup', 'golden_tenktup1', 'golden_hundredktup1']
        for i in range(len(scaleup)):
            scaleup_raw[scaleup[i]] = []

            for j in range(10):
                cur.execute(
                    " select l.unique1, r.unique1, rr.unique1"
                    " from "+temp_schema+"." + scaleup[i] +
                    " l join "+temp_schema+"." + scaleup[i] + " r on l.unique2 = r.unique2 " +
                    "join ipercy." + scaleup[i] + " rr  on l.unique2 = rr.unique2")
                result = cur.fetchall()
                time = result[0][1]
                scaleup_raw[scaleup[i]].append(time)
            scaleup_raw[scaleup[i]].remove(max(scaleup_raw[scaleup[i]]))
            scaleup_raw[scaleup[i]].remove(min(scaleup_raw[scaleup[i]]))
            all_results["test4-"+scaleup[i]] = np.array(scaleup_raw[scaleup[i]]).mean().round(decimals=1)
        # print ("Scaleup test averages", scaleup_results)
        # print ("Update averages", update_results)
        print ("All test Averages", all_results)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))


def disconnect(cur, db):
    try:
        cur.execute("show tables")
        tables=cur.fetchall()
        cur.execute("drop table if exists onektup")
        cur.execute("drop table if exists tenktup1")
        cur.execute("drop table if exists tenktup2")

        for table in tables:
            print(table)
        db.close()
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))


def main():
    temp_schema = 'dbgroup2'
    base_table = "golden_tenktup1"
    cloned_tables = ["tenktup1","tenktup2","onektup"]
    cur = connect()
    cloneTables(cur[0], temp_schema, base_table, cloned_tables)
    queries(cur[0], temp_schema,base_table, cloned_tables)
    disconnect(cur[0], cur[1])
    print("Disconnected")



if __name__ == "__main__":
    main()
