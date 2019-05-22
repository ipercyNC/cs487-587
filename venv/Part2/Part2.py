import csv
import random
import sys
import Queries
import json

from config import pgSQLconfig
from config import mySQLconfig
import mysql.connector
import psycopg2


def filenameIterate(index):
    tableList = {1: {"count":1000, "name":"onektup"},
                 2: {"count":10000, "name":"tenktup1"},
                 3: {"count":10000, "name":"tenktup2"},
                 4: {"count":100000, "name":"hundredktup1"},
                 5: {"count":100000, "name":"hundredktup2"},
                 6: {"count":1000000, "name":"onemtup1"},
                 7: {"count":1000000, "name":"onemtup2"},
                 8: {"count":10000000, "name":"tenmtup1"},
                 9: {"count":10000000, "name":"tenmtup2"}
    }
    return tableList[index]


def pgSQLconnect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = pgSQLconfig()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS proj CASCADE")
        cur.execute("CREATE SCHEMA IF NOT EXISTS proj")
        cur.execute("SET search_path TO proj")
        conn.commit()
        return cur, conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def pgSQLdisconnect(cur, conn):
    try:
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def mySQLconnect():
    """ Connect to the mySQL database server """
    conn = None
    try:
        # read connection parameters
        params = mySQLconfig()
        # connect to the MySQL server

        print('Connecting to the MySQL database...')
        conn = mysql.connector.connect(**params)
        cur = conn.cursor()
        cur.execute("SET profiling = 1;")
        cur.execute("set profiling_history_size =1")

        for i in range(1,6):
            rec = filenameIterate(i)
            cur.execute("DROP TABLE IF EXISTS %s CASCADE" % rec['name'])

        """Loop through clearing project tables and copying data over from the golden tables"""
        """
        for i in range(1,8):
            rec = filenameIterate(i)
            cur.execute("TRUNCATE TABLE IF EXISTS %s CASCADE" % rec['name'])
            cur.execute("")
            """

        return cur, conn
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))


def mySQLdisconnect(cur, conn):
    try:

        for i in range(1,6):
            rec = filenameIterate(i)
            cur.execute("DROP TABLE IF EXISTS %s CASCADE" % rec['name'] )

        # close the communication with the MySQL
        cur.close()
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
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


def datagenfile(cur, count, filename, conn):
    cur.execute("drop table if exists " + str(filename))
    cur.execute("CREATE TABLE " + str(filename) + "("
        "unique1 integer NOT NULL,"
        "unique2 integer PRIMARY KEY,"
        "two integer NOT NULL,"
        "four integer NOT NULL,"
        "ten integer NOT NULL,"
        "twenty integer NOT NULL,"
        "onePercent integer NOT NULL,"
        "tenPercent integer NOT NULL,"
        "twentyPercent integer NOT NULL,"
        "fiftyPercent integer NOT NULL,"
        "unique3 integer NOT NULL,"
        "evenOnePercent integer NOT NULL,"
        "oddOnePercent integer NOT NULL,"
        "stringu1 VARCHAR(52) NOT NULL,"
        "stringu2 VARCHAR(52) NOT NULL,"
        "string4 VARCHAR(52) NOT NULL,"
        "UNIQUE(unique1,unique2,unique3)"
        ")")

    tupleCount = count
    numList = random.sample(range(0, tupleCount), tupleCount)
    lineToInsert = ''
    for i in range(tupleCount):
        if (i != 0 and (i - 1) % 100 != 0):
            lineToInsert += ","
        unique1 = numList[i]
        unique2 = i
        two = unique1 % 2
        four = unique1 % 4
        ten = unique1 % 10
        twenty = unique1 % 20
        onePercent = unique1 % 100
        tenPercent = unique1 % 10
        twentyPercent = unique1 % 5
        fiftyPercent = unique1 % 2
        unique3 = unique1
        evenOnePercent = onePercent * 2
        oddOnePercent = onePercent * 2 + 1
        stringu1 = convert(unique1)
        stringu2 = convert(unique2)
        string4 = str4Select(i)
        lineToInsert += ("(" + str(unique1) + "," + str(unique2) + "," + str(two) + "," + str(four) + "," + str(ten) +
                         "," + str(twenty) + "," + str(onePercent) + "," + str(tenPercent) + "," + str(twentyPercent) +
                         "," + str(fiftyPercent) + "," + str(unique3) + "," + str(evenOnePercent) + "," +
                         str(oddOnePercent) + ",'" + str(stringu1) + "','" + str(stringu2) + "','" + str(string4) + "')")

        if i % 100 == 0:
            cur.execute("insert into " + str(filename) + "(unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                                         "twentyPercent,fiftyPercent,unique3,evenOnePercent,oddOnePercent,"
                                                         "stringu1,stringu2,string4) values " + lineToInsert + "")
            lineToInsert = ''
            conn.commit()
        conn.commit()
    cur.execute("insert into " + str(filename) + "(unique1,unique2,two,four,ten,twenty,onePercent,tenPercent,"
                                                 "twentyPercent,fiftyPercent,unique3,evenOnePercent,oddOnePercent,"
                                                 "stringu1,stringu2,string4) values " + lineToInsert + "")
    conn.commit()

"""Copies tables from the "golden" schema into the project schema"""
def pgSQLimportData(cur, conn):
    print("Creating onektup table")
    cur.execute("CREATE TABLE onektup AS TABLE golden.onektup")
    conn.commit()
    print("Creating tenktup1 table")
    cur.execute("CREATE TABLE tenktup1 AS TABLE golden.tenktup1")
    conn.commit()
    print("Creating tenktup2 table")
    cur.execute("CREATE TABLE tenktup2 AS TABLE golden.tenktup2")
    conn.commit()
    print("Creating hundredktup1 table")
    cur.execute("CREATE TABLE hundredktup1 AS TABLE golden.hundredktup1")
    conn.commit()
    print("Creating hundredktup2 table")
    cur.execute("CREATE TABLE hundredktup2 AS TABLE golden.hundredktup2")
    conn.commit()
    """
    print("Creating onemtup1 table")
    cur.execute("CREATE TABLE onemtup1 AS TABLE golden.onemtup1")
    conn.commit()
    print("Creating onemtup2 table")
    cur.execute("CREATE TABLE onemtup2 AS TABLE golden.onemtup2")
    conn.commit()
    print("Creating tenmtup1 table")
    cur.execute("CREATE TABLE tenmtup1 AS TABLE golden.tenmtup1")
    conn.commit()
    print("Creating tenmtup2 table")
    cur.execute("CREATE TABLE tenmtup2 AS TABLE golden.tenmtup2")
    conn.commit()
    """

"""Copies tables from the "golden" schema into the project schema"""
def mySQLimportData(cur, conn):
    print("Creating onektup table")
    cur.execute("CREATE TABLE IF NOT EXISTS onektup LIKE golden_onektup")
    cur.execute("INSERT onektup select * from golden_onektup")
    conn.commit()
    print("Creating tenktup1 table")
    cur.execute("CREATE TABLE IF NOT EXISTS tenktup1 LIKE golden_tenktup1")
    cur.execute("INSERT tenktup1 select * from golden_tenktup1")
    conn.commit()
    print("Creating tenktup2 table")
    cur.execute("CREATE TABLE IF NOT EXISTS tenktup2 LIKE golden_tenktup2")
    cur.execute("INSERT tenktup2 select * from golden_tenktup2")
    conn.commit()
    print("Creating hundredktup1 table")
    cur.execute("CREATE TABLE IF NOT EXISTS hundredktup1 LIKE golden_hundredktup1")
    cur.execute("INSERT hundredktup1 select * from golden_hundredktup1")
    conn.commit()
    print("Creating hundredktup2 table")
    cur.execute("CREATE TABLE IF NOT EXISTS hundredktup2 LIKE golden_hundredktup2")
    cur.execute("INSERT hundredktup2 select * from golden_hundredktup2")
    conn.commit()
    """
    print("Creating onemtup1 table")
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS onemtup1 LIKE golden_onemtup1")
    cur.execute("INSERT onemtup1 select * from golden_onemtup1")
    conn.commit()
    print("Creating onemtup2 table")
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS onemtup2 LIKE golden_onemtup2")
    cur.execute("INSERT onemtup2 select * from golden_onemtup2")
    conn.commit()
    print("Creating tenmtup1 table")
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS tenmtup1 LIKE golden_tenmtup1")
    cur.execute("INSERT tenmtup1 select * from golden_tenmtup1")
    conn.commit()
    print("Creating tenmtup2 table")
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS tenmtup2 LIKE golden_tenmtup2")
    cur.execute("INSERT tenmtup2 select * from golden_tenmtup2")
    conn.commit()
    """

def main():
    for i in range (11):
        print("Data collection run#:\t%d"%i)
        pgSQLconn = None
        mySQLconn = None

        #create the postgresql cursor and connection
        pgSQLcur, pgSQLconn = pgSQLconnect()

        #create the mysql cursor and connection
        mySQLcur, mySQLconn = mySQLconnect()


        """Clone the default data into the temp schema tables"""
        print("Setting up project tables in PostgreSQL")
        pgSQLimportData(pgSQLcur, pgSQLconn)
        """"Modify the default location to match """
        pgSQLcur.execute("SET search_path TO proj")

        print("Setting up project tables in MySQL")
        mySQLimportData(mySQLcur, mySQLconn)

        """Create the BPRIME table used for queries"""
        """Modify the following function call to get the sizes for the bprime"""
        size = filenameIterate(3)
        print("Creating Bprime table in PostgreSQL")
        Queries.createbprime(pgSQLcur, pgSQLconn, size['name'], size['count']/10)
        pgSQLconn.commit()
        print("Creating bprime table in mySQL")
        Queries.mySQLcreatebprime(mySQLcur, mySQLconn, size['name'], size['count']/10)
        mySQLconn.commit()

        #Part 1 queries - checking bulk updates of tables
        #Postgres queries
        print("Part 1 Queries")
        print("PostgreSQL Queries")
        print("50% Update Query")
        Queries.fiftypercentupdate(pgSQLcur, pgSQLconn, "onektup")
        print("75% Update Query")
        Queries.seventyfivepercentupdate(pgSQLcur, pgSQLconn, "onektup")
        print("100% Update Query")
        Queries.hundredpercentupdate(pgSQLcur, pgSQLconn, "onektup")
        print("Bulk Join Update Query")
        Queries.bulkjoinupdate(pgSQLcur, pgSQLconn, "onektup")
        print("Index Update Query")
        Queries.indexupdate(pgSQLcur, pgSQLconn, "onektup")

        #MySQL queries
        print("MySQL Queries")
        print("50% Update Query")
        Queries.mySQLfiftypercentupdate(mySQLcur, mySQLconn, "onektup")
        print("75% Update Query")
        Queries.mySQLseventyfivepercentupdate(mySQLcur, mySQLconn, "onektup")
        print("100% Update Query")
        Queries.mySQLhundredpercentupdate(mySQLcur, mySQLconn, "onektup")
        print("Bulk Join Update Query")
        Queries.mySQLbulkjoinupdate(mySQLcur, mySQLconn, "onektup")
        print("Index Update Query")
        Queries.mySQLindexupdate(mySQLcur, mySQLconn, "onektup")

        #Part 2 queries - Checking Join algorithm performance
        #Postgres queries
        print("Part 2 queries")
        print("Postgres Queries")
        print("Query13")
        Queries.query13(pgSQLcur, pgSQLconn, "hundredktup1")
        print("Query14")
        Queries.query14(pgSQLcur, pgSQLconn, "onektup", "tenktup1", "tenktup2")

        #MySQL queries
        print("MySQL Queries")
        print("Query13")
        Queries.mySQLquery13(mySQLcur, mySQLconn, "hundredktup1")
        print("Query14")
        Queries.mySQLquery14(mySQLcur, mySQLconn, "onektup", "tenktup1", "tenktup2")

        #Part 3 queries - Test the use of partial indicies
        #Variable used to hold table name to create index on
        print("Part 3 Queries")
        print("Postgres Queries")
        print("No partial index Performance Query")
        Queries.nopartialindexperf(pgSQLcur, pgSQLconn, "hundredktup1")
        tableindex = "hundredktup1"
        pgSQLcur.execute("CREATE INDEX idx_two_value_0 "
                    "ON %s(ten) "
                    "WHERE ten <= 3" % tableindex)
        pgSQLconn.commit()
        print("Partial Index Perfomance Query")
        Queries.partialindexperf(pgSQLcur, pgSQLconn, "hundredktup1")
        pgSQLcur.execute("DROP INDEX IF EXISTS idx_ten_value_3 CASCADE")


        #MySQL Queries
        print("MySQL Queries")
        print("No partial index Performance Query")
        Queries.mySQLnopartialindexperf(mySQLcur, mySQLconn, "hundredktup1")
        print("Partial Index Perfomance Query")
        Queries.mySQLpartialindexperf(mySQLcur, mySQLconn, "hundredktup1")


        #Part 4 query - Test the performance of three way join
        #Postgres query
        print("Part 4 Queries")
        print("Postgres Queries")
        for i in range(1, 6):
            rec = filenameIterate(i)
            print("3way Join %s Query" % rec['name'])
            Queries.threewayjoin(pgSQLcur, pgSQLconn, rec['name'])

        #MySQL Queries
        print("MySQL Queries")
        for i in range(1, 6):
            rec = filenameIterate(i)
            print("3way Join %s Query" % rec['name'])
            Queries.mySQLthreewayjoin(mySQLcur, mySQLconn, rec['name'])

        pgSQLdisconnect(pgSQLcur, pgSQLconn)
        print("Disconnected from PostgreSQL")

        mySQLdisconnect(mySQLcur, mySQLconn)
        print("Disconnected from MySQL")


def generate():
    conn = None
    cur, conn = mySQLconnect()
    for i in range(6,7):
        rec = filenameIterate(i)
        # print(rec['count'], rec['name'])
        datagenfile(cur, rec['count'], rec['name'], conn)

    mySQLdisconnect(cur, conn)
    print("Disconnected")


def pgtest():
    for i in range(1):
        pgSQLconn = None

        #create the postgresql cursor and connection
        pgSQLcur, pgSQLconn = pgSQLconnect()

        """Clone the default data into the temp schema tables"""
        print("Setting up project tables in PostgreSQL")
        pgSQLimportData(pgSQLcur, pgSQLconn)
        """"Modify the default location to match """
        pgSQLcur.execute("SET search_path TO proj")

        """Create the BPRIME table used for queries"""
        """Modify the following function call to get the sizes for the bprime"""
        size = filenameIterate(3)
        print("Creating Bprime table in PostgreSQL")
        Queries.createbprime(pgSQLcur, pgSQLconn, size['name'], size['count']/10)
        pgSQLconn.commit()

        #
        # #Part 1 queries - checking bulk updates of tables
        # #Postgres queries
        # print("Part 1 Queries")
        # print("PostgreSQL Queries")
        # print("50% Update Query")
        # Queries.fiftypercentupdate(pgSQLcur, pgSQLconn, "onektup")
        # print("75% Update Query")
        # Queries.seventyfivepercentupdate(pgSQLcur, pgSQLconn, "onektup")
        # print("100% Update Query")
        # Queries.hundredpercentupdate(pgSQLcur, pgSQLconn, "onektup")
        # print("Bulk Join Update Query")
        # Queries.bulkjoinupdate(pgSQLcur, pgSQLconn, "onektup")
        # print("Index Update Query")
        # Queries.indexupdate(pgSQLcur, pgSQLconn, "onektup")
        #
        #
        # #Part 2 queries - Checking Join algorithm performance
        # #Postgres queries
        # print("Part 2 queries")
        # print("Postgres Queries")
        # print("Query13")
        # Queries.query13(pgSQLcur, pgSQLconn, "hundredktup1")
        # print("Query14")
        # Queries.query14(pgSQLcur, pgSQLconn, "tenktup1", "hundredktup1", "hundredktup2")
        #

        #Part 3 queries - Test the use of partial indicies
        #Variable used to hold table name to create index on
        print("Part 3 Queries")
        print("Postgres Queries")
        print("No partial index Performance Query")
        tableindex = "hundredktup1"
        try:

            pgSQLcur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                        "SELECT * "
                        "FROM %s "
                        "WHERE ten =0" % tableindex
                        )
            row = pgSQLcur.fetchall()
            with open("pgQueryPartialIndexNotUsed%s.txt" % tableindex, mode='a', newline='') as query_file:
                json.dump(row, query_file, sort_keys=True, indent=4)
            query_file.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


        pgSQLcur.execute("CREATE INDEX idx_ten_value_0 "
                    "ON %s(ten) "
                    "WHERE ten <=5" % tableindex)
        pgSQLconn.commit()
        print("Partial Index Perfomance Query")
        try:

            pgSQLcur.execute("EXPLAIN (ANALYZE, BUFFERS) "
                        "SELECT * "
                        "FROM %s "
                        "WHERE ten =1 " % tableindex
                        )
            row = pgSQLcur.fetchall()
            with open("pgQueryPartialIndexUsed%s.txt" % tableindex, mode='a', newline='') as query_file:
                json.dump(row, query_file, sort_keys=True, indent=4)
            query_file.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        pgSQLcur.execute("DROP INDEX IF EXISTS idx_two_value_0 CASCADE")

        # #Part 4 query - Test the performance of three way join
        # #Postgres query
        # print("Part 4 Queries")
        # print("Postgres Queries")
        # for i in range(1, 6):
        #     rec = filenameIterate(i)
        #     print("3way Join %s Query" % rec['name'])
        #     Queries.threewayjoin(pgSQLcur, pgSQLconn, rec['name'])

        pgSQLdisconnect(pgSQLcur, pgSQLconn)
        print("Disconnected from PostgreSQL")


def mytest():
    mySQLconn = None

    # create the mysql cursor and connection
    mySQLcur, mySQLconn = mySQLconnect()

    """Clone the default data into the temp schema tables"""
    print("Setting up project tables in MySQL")
    mySQLimportData(mySQLcur, mySQLconn)

    """Create the BPRIME table used for queries"""
    """Modify the following function call to get the sizes for the bprime"""
    size = filenameIterate(3)
    print("Creating bprime table in mySQL")
    Queries.mySQLcreatebprime(mySQLcur, mySQLconn, size['name'], size['count'] / 10)
    mySQLconn.commit()

    # Part 1 queries - checking bulk updates of tables
    # MySQL queries
    print("MySQL Queries")
    print("50% Update Query")
    Queries.mySQLfiftypercentupdate(mySQLcur, mySQLconn, "onektup")
    mySQLconn.commit()
    print("75% Update Query")
    Queries.mySQLseventyfivepercentupdate(mySQLcur, mySQLconn, "onektup")
    print("100% Update Query")
    Queries.mySQLhundredpercentupdate(mySQLcur, mySQLconn, "onektup")
    print("Bulk Join Update Query")
    Queries.mySQLbulkjoinupdate(mySQLcur, mySQLconn, "onektup")
    print("Index Update Query")
    Queries.mySQLindexupdate(mySQLcur, mySQLconn, "onektup")

    # Part 2 queries - Checking Join algorithm performance
    print("Part 2 queries")
    # MySQL queries
    print("MySQL Queries")
    print("Query13")
    Queries.mySQLquery13(mySQLcur, mySQLconn, "hundredktup1")
    print("Query14")
    Queries.mySQLquery14(mySQLcur, mySQLconn, "onektup", "tenktup1", "tenktup2")

    # Part 3 queries - Test the use of partial indicies
    # Variable used to hold table name to create index on
    print("Part 3 Queries")
    # MySQL Queries
    print("MySQL Queries")
    print("No partial index Performance Query")
    Queries.mySQLnopartialindexperf(mySQLcur, mySQLconn, "hundredktup1")
    print("Partial Index Perfomance Query")
    Queries.mySQLpartialindexperf(mySQLcur, mySQLconn, "hundredktup1")

    # Part 4 query - Test the performance of three way join
    # MySQL Queries
    print("MySQL Queries")
    for i in range(1, 6):
        rec = filenameIterate(i)
        print("3way Join %s Query" % rec['name'])
        Queries.mySQLthreewayjoin(mySQLcur, mySQLconn, rec['name'])

    mySQLdisconnect(mySQLcur, mySQLconn)
    print("Disconnected from MySQL")


if __name__ == "__main__":
    main()
    #generate()
    #pgtest()
    #mytest()