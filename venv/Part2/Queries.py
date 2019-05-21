#!/usr/bin/python
import csv
import json
import psycopg2

import config


def createbprime(cur, conn, table, size):
    """Create the bprime tables"""
    commands = """
        CREATE TABLE bprime (
            unique1 integer NOT NULL,
            unique2 integer PRIMARY KEY,
            two integer NOT NULL,
            four integer NOT NULL,
            ten integer NOT NULL,
            twenty integer NOT NULL,
            onepercent integer NOT NULL,
            tenpercent integer NOT NULL,
            twentypercent integer NOT NULL,
            fiftypercent integer NOT NULL,
            unique3 integer NOT NULL,
            evenonepercent integer NOT NULL,
            oddonepercent integer NOT NULL,
            stringu1 varchar(52) NOT NULL,
            stringu2 varchar(52) NOT NULL,
            string4 varchar(52) NOT NULL
        )
        """
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute(commands)
        cur.execute("INSERT INTO bprime SELECT * FROM %s WHERE %s.unique2 < %d" % (table, table, size))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 1 for part 1:  50% selectivity update"""
def fiftypercentupdate(cur, conn, table):

    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "UPDATE %s "
                    "SET two = 1 "
                    "WHERE four <=1" % table
                    )
        conn.commit()
        row = cur.fetchall()
        with open("Query50SelectUpdate.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent = 4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

"""Query 2 for part 1:  75% selectivity update"""
def seventyfivepercentupdate(cur, conn, table):

    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "UPDATE %s "
                    "SET two = 1 "
                    "WHERE four <=2" % table
                    )
        conn.commit()
        row = cur.fetchall()
        with open("Query75SelectUpdate.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 3 for part 1:  100% selectivity update"""
def hundredpercentupdate(cur, conn, table):

    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "UPDATE %s "
                    "SET two = 1 "
                    "WHERE four <=3" % table
                    )
        conn.commit()
        row = cur.fetchall()
        with open("Query100SelectUpdate.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 4 for part 1:  Bulk update after a join, using 2 tables of same size"""
def bulkjoinupdate(cur, conn, table1, table2):
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "UPDATE %s "
                    "SET two = 1 "
                    "FROM %s as a "
                    "JOIN %s as b "
                    "ON a.four = b.four "
                    "WHERE b.four = 0 " % (table1, table1, table2)
                    )
        conn.commit()
        row = cur.fetchall()
        with open("QueryJoinUpdate.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 5 for part 1:  Bulk update on an index"""
def indexupdate(cur, conn, table):
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "UPDATE %s "
                    "SET unique2 = unique1" % table
                    )
        conn.commit()
        row = cur.fetchall()
        with open("QueryIndexUpdate.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 1 for part 3:  Partial index performance"""
def partialindexperf(cur, conn, table):
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "SELECT * "
                    "FROM %s "
                    "WHERE two = 0" % table
                    )
        row = cur.fetchall()
        with open("QueryPartialIndexUsed.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query 2 for part 3:  Partial index performance"""
def nopartialindexperf(cur, conn, table):
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "SELECT * "
                    "FROM %s "
                    "WHERE two = 1" % table
                    )
        row = cur.fetchall()
        with open("QueryPartialIndexNotUsed.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""Query for part 4:  3 way join performance"""
def threewayjoin(cur, conn, table):
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "SELECT a.unique1, b.unique1, c.unique1 "
                    "FROM %s as a "
                    "JOIN %s as b "
                    "ON a.unique2 = b.unique2 "
                    "JOIN %s as c "
                    "ON a.unique2 = c.unique2" % (table, table, table)
                    )
        row = cur.fetchall()
        with open(("QueryThreeWayJoin%s.txt"% table), mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

"""Query13 - (clustered index) - JoinABprime"""
def query13(cur, conn, table1):
    """Create the TMP table to hold query results"""
    commands = """
                 CREATE TEMP TABLE query13 (
                     unique1 integer NOT NULL,
                     unique2 integer NOT NULL,
                     two integer NOT NULL,
                     four integer NOT NULL,
                     ten integer NOT NULL,
                     twenty integer NOT NULL,
                     onepercent integer NOT NULL,
                     tenpercent integer NOT NULL,
                     twentypercent integer NOT NULL,
                     fiftypercent integer NOT NULL,
                     unique3 integer NOT NULL,
                     evenonepercent integer NOT NULL,
                     oddonepercent integer NOT NULL,
                     stringu1 character(52) NOT NULL,
                     stringu2 character(52) NOT NULL,
                     string4 character(52) NOT NULL,
                     tunique1 integer NOT NULL,
                     tunique2 integer NOT NULL,
                     ttwo integer NOT NULL,
                     tfour integer NOT NULL,
                     tten integer NOT NULL,
                     ttwenty integer NOT NULL,
                     tonepercent integer NOT NULL,
                     ttenpercent integer NOT NULL,
                     ttwentypercent integer NOT NULL,
                     tfiftypercent integer NOT NULL,
                     tunique3 integer NOT NULL,
                     tevenonepercent integer NOT NULL,
                     toddonepercent integer NOT NULL,
                     tstringu1 character(52) NOT NULL,
                     tstringu2 character(52) NOT NULL,
                     tstring4 character(52) NOT NULL
                 )
                 """
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute(commands)
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "INSERT INTO query13 "
                    "SELECT * "
                    "FROM %s as t1, bprime "
                    "WHERE (t1.unique2 = bprime.unique2)" % table1)
        conn.commit()
        row = cur.fetchall()
        with open("query13.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

"""Query14 - (clustered index) - JoinCselAselB"""

def query14(cur, conn, table1, table2, table3):
    commands = """
                   CREATE TEMP TABLE query14 (
                       unique1 integer NOT NULL,
                       unique2 integer NOT NULL,
                       two integer NOT NULL,
                       four integer NOT NULL,
                       ten integer NOT NULL,
                       twenty integer NOT NULL,
                       onepercent integer NOT NULL,
                       tenpercent integer NOT NULL,
                       twentypercent integer NOT NULL,
                       fiftypercent integer NOT NULL,
                       unique3 integer NOT NULL,
                       evenonepercent integer NOT NULL,
                       oddonepercent integer NOT NULL,
                       stringu1 character(52) NOT NULL,
                       stringu2 character(52) NOT NULL,
                       string4 character(52) NOT NULL,
                       aunique1 integer NOT NULL,
                       aunique2 integer NOT NULL,
                       atwo integer NOT NULL,
                       afour integer NOT NULL,
                       aten integer NOT NULL,
                       atwenty integer NOT NULL,
                       aonepercent integer NOT NULL,
                       atenpercent integer NOT NULL,
                       atwentypercent integer NOT NULL,
                       afiftypercent integer NOT NULL,
                       aunique3 integer NOT NULL,
                       aevenonepercent integer NOT NULL,
                       aoddonepercent integer NOT NULL,
                       astringu1 character(52) NOT NULL,
                       astringu2 character(52) NOT NULL,
                       astring4 character(52) NOT NULL,
                       bunique1 integer NOT NULL,
                       bunique2 integer NOT NULL,
                       btwo integer NOT NULL,
                       bfour integer NOT NULL,
                       bten integer NOT NULL,
                       btwenty integer NOT NULL,
                       bonepercent integer NOT NULL,
                       btenpercent integer NOT NULL,
                       btwentypercent integer NOT NULL,
                       bfiftypercent integer NOT NULL,
                       bunique3 integer NOT NULL,
                       bevenonepercent integer NOT NULL,
                       boddonepercent integer NOT NULL,
                       bstringu1 character(52) NOT NULL,
                       bstringu2 character(52) NOT NULL,
                       bstring4 character(52) NOT NULL
                   )
                   """
    try:
        #cur.execute("SET search_path TO proj")
        cur.execute(commands)
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                    "INSERT INTO query14 "
                    "SELECT * "
                    "FROM %s as t1, %s as t2, %s as t3 "
                    "WHERE (t1.unique2=t2.unique2) "
                    "AND (t2.unique2 = t3.unique2) "
                    "AND (t2.unique2 <10000)" % (table1, table2, table3))
        conn.commit()
        row = cur.fetchall()
        with open("query14.txt", mode='a', newline='') as query_file:
            json.dump(row, query_file, sort_keys=True, indent=4)
        query_file.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

