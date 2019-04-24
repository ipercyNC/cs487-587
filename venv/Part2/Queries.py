#!/usr/bin/python
import csv

import psycopg2

from config import config


def createtmp():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TABLE cs487.tmp (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query1 - no index set 1% selection"""


def query1():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query1 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query1 SELECT * FROM cs487.tenktup1 WHERE unique2 BETWEEN 0 AND 99")
        conn.commit()
        cur.execute("SELECT * FROM query1")
        row = cur.fetchone()
        with open("query1.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query2 - No Index set 10% selection"""


def query2():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query2 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query2 SELECT * FROM cs487.tenktup1 WHERE unique2 BETWEEN 792 AND 1791")
        conn.commit()
        cur.execute("SELECT * FROM query2")
        row = cur.fetchone()
        with open("query2.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query3 - clustered Index 1% selection"""


def query3():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query3 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query3 SELECT * FROM cs487.tenktup1 WHERE unique2 BETWEEN 0 AND 99")
        conn.commit()
        cur.execute("SELECT * FROM query3")
        row = cur.fetchone()
        with open("query3.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query4 - clustered index 10% selection"""


def query4():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query4 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query4 SELECT * FROM cs487.tenktup1 WHERE unique2 BETWEEN 791 AND 1791")
        conn.commit()
        cur.execute("SELECT * FROM query4")
        row = cur.fetchone()
        with open("query4.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query5 - 1% selection via a non-clustered index"""


def query5():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query5 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query5 SELECT * FROM cs487.tenktup1 WHERE unique1 BETWEEN 0 AND 99")
        conn.commit()
        cur.execute("SELECT * FROM query5")
        row = cur.fetchone()
        with open("query5.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query6 - 10% selection via a non-clustered index"""


def query6():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query6 (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query6 SELECT * FROM cs487.tenktup1 WHERE unique1 BETWEEN 792 AND 1791")
        conn.commit()
        cur.execute("SELECT * FROM query6")
        row = cur.fetchone()
        with open("query6.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query7 - single tuple selection via clustered index to screen"""


def query7():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE unique2 = 2001")
        row = cur.fetchone()
        with open("query7.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query8 - 1% selection via clustered index to screen"""


def query8():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE unique2 BETWEEN 0 AND 99")
        row = cur.fetchone()
        with open("query8.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query9 - (no index) JoinAselB"""


def query9():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query9 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query9 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, "
                    "twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, "
                    "string4, tunique1, tunique2, ttwo, tfour, tten, ttwenty, tonePercent, ttenPercent, ttwentyPercent,"
                    " tfiftyPercent, tunique3, tevenOnePercent, toddOnePercent, tstringu1, tstringu2, tstring4) "
                    "SELECT * FROM cs487.tenktup1, cs487.tenktup2 WHERE (cs487.tenktup1.unique2 = "
                    "cs487.tenktup2.unique2) AND (cs487.tenktup2.unique2 < 1000)")
        conn.commit()
        cur.execute("SELECT * FROM query9")
        row = cur.fetchone()
        with open("query9.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query10 - (no index) - JoinABprime"""


def query10():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query10 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query10 S"
                    "ELECT * "
                    "FROM cs487.tenktup1, cs487.bprime "
                    "WHERE (cs487.tenktup1.unique2 = cs487.bprime.unique2)")
        conn.commit()
        cur.execute("SELECT * FROM query10")
        row = cur.fetchone()
        with open("query10.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query11 - (no index) - JoinCselAselB"""


def query11():
    commands = """
       CREATE TEMP TABLE query11 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(  # EXPLAIN (ANALYZE, BUFFERS)
            "INSERT INTO query11 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, "
            "twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, "
            "string4, aunique1, aunique2, atwo, afour, aten, atwenty, aonePercent, atenPercent, atwentyPercent,"
            " afiftyPercent, aunique3, aevenOnePercent, aoddOnePercent, astringu1, astringu2, astring4, bunique1, "
            "bunique2, btwo, bfour, bten, btwenty, bonePercent, btenPercent, btwentyPercent,"
            " bfiftyPercent, bunique3, bevenOnePercent, boddOnePercent, bstringu1, bstringu2, bstring4) "
            "SELECT * FROM cs487.onektup, cs487.tenktup1, cs487.tenktup2 "
            "WHERE (cs487.onektup.unique2=cs487.tenktup1.unique2) "
            "AND (cs487.tenktup1.unique2 = cs487.tenktup2.unique2)"
            "AND (cs487.tenktup1.unique2 <1000)")
        conn.commit()
        cur.execute("SELECT * FROM query11")
        row = cur.fetchone()
        with open("query11.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query12 - (clustered index) - JoinAselB"""


def query12():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query12 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query12 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4, tunique1, tunique2, ttwo, tfour, tten, ttwenty, tonePercent, ttenPercent, ttwentyPercent, tfiftyPercent, tunique3, tevenOnePercent, toddOnePercent, tstringu1, tstringu2, tstring4) SELECT * FROM cs487.tenktup1, cs487.tenktup2 WHERE (cs487.tenktup1.unique2 = cs487.tenktup2.unique2) AND (cs487.tenktup2.unique2 < 1000)")
        conn.commit()
        cur.execute("SELECT * FROM query12")
        row = cur.fetchone()
        with open("query12.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query13 - (clustered index) - JoinABprime"""


def query13():
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO query13 "
                    "SELECT * "
                    "FROM cs487.tenktup1, cs487.bprime "
                    "WHERE (cs487.tenktup1.unique2 = cs487.bprime.unique2)")
        conn.commit()
        cur.execute("SELECT * FROM query13")
        row = cur.fetchone()
        with open("query13.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query14 - (clustered index) - JoinCselAselB"""


def query14():
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(  # EXPLAIN (ANALYZE, BUFFERS)
            "INSERT INTO query14 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, "
            "twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, "
            "string4, aunique1, aunique2, atwo, afour, aten, atwenty, aonePercent, atenPercent, atwentyPercent,"
            " afiftyPercent, aunique3, aevenOnePercent, aoddOnePercent, astringu1, astringu2, astring4, bunique1, "
            "bunique2, btwo, bfour, bten, btwenty, bonePercent, btenPercent, btwentyPercent,"
            " bfiftyPercent, bunique3, bevenOnePercent, boddOnePercent, bstringu1, bstringu2, bstring4) "
            "SELECT * FROM cs487.onektup, cs487.tenktup1, cs487.tenktup2 "
            "WHERE (cs487.onektup.unique2=cs487.tenktup1.unique2) "
            "AND (cs487.tenktup1.unique2 = cs487.tenktup2.unique2)"
            "AND (cs487.tenktup1.unique2 <1000)")
        conn.commit()
        cur.execute("SELECT * FROM query14")
        row = cur.fetchone()
        with open("query14.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query15 - (non-clustered index) - JoinAselB"""


def query15():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query15 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query15 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, "
            "fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4, tunique1, tunique2, "
            "ttwo, tfour, tten, ttwenty, tonePercent, ttenPercent, ttwentyPercent, tfiftyPercent, tunique3, "
            "tevenOnePercent, toddOnePercent, tstringu1, tstringu2, tstring4) "
            "SELECT * FROM cs487.tenktup1, cs487.tenktup2 "
            "WHERE (cs487.tenktup1.unique1 = cs487.tenktup2.unique1) "
            "AND (cs487.tenktup1.unique2 < 1000)")
        conn.commit()
        cur.execute("SELECT * FROM query15")
        row = cur.fetchone()
        with open("query15.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query16 - (non-clustered index) - JoinABprime"""


def query16():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TEMP TABLE query16 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query16 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, "
            "fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4, tunique1, tunique2, "
            "ttwo, tfour, tten, ttwenty, tonePercent, ttenPercent, ttwentyPercent, tfiftyPercent, tunique3, "
            "tevenOnePercent, toddOnePercent, tstringu1, tstringu2, tstring4) "
            "SELECT * FROM cs487.tenktup1, cs487.tenktup2 "
            "WHERE (cs487.tenktup1.unique1 = cs487.tenktup2.unique1) "
            "AND (cs487.tenktup1.unique2 < 1000)")
        conn.commit()
        cur.execute("SELECT * FROM query16")
        row = cur.fetchone()
        with open("query16.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query17 - (non-clustered index) - JoinCselAselB"""


def query17():
    commands = """
        CREATE TEMP TABLE query17 (
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
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(  # EXPLAIN (ANALYZE, BUFFERS)
            "INSERT INTO query17 (unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, "
            "twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, "
            "string4, aunique1, aunique2, atwo, afour, aten, atwenty, aonePercent, atenPercent, atwentyPercent,"
            " afiftyPercent, aunique3, aevenOnePercent, aoddOnePercent, astringu1, astringu2, astring4, bunique1, "
            "bunique2, btwo, bfour, bten, btwenty, bonePercent, btenPercent, btwentyPercent,"
            " bfiftyPercent, bunique3, bevenOnePercent, boddOnePercent, bstringu1, bstringu2, bstring4) "
            "SELECT * FROM cs487.onektup, cs487.tenktup1, cs487.tenktup2 "
            "WHERE (cs487.onektup.unique1=cs487.tenktup1.unique1) "
            "AND (cs487.tenktup1.unique1 = cs487.tenktup2.unique1)"
            "AND (cs487.tenktup1.unique1 <1000)")
        conn.commit()
        cur.execute("SELECT * FROM query17")
        row = cur.fetchone()
        with open("query17.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query18 - Projection with 1% Projection"""


def query18():
    commands = """
        CREATE TEMP TABLE query18 (
            two integer NOT NULL,
            four integer NOT NULL,
            ten integer NOT NULL,
            twenty integer NOT NULL,
            onepercent integer NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(  # EXPLAIN (ANALYZE, BUFFERS)
            "INSERT INTO query18 (two, four, ten, twenty, onepercent, string4) "
            "SELECT DISTINCT two, four, ten, twenty, onepercent, string4 "
            "FROM cs487.tenktup1")
        conn.commit()
        cur.execute("SELECT * FROM query18")
        row = cur.fetchone()
        with open("query18.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query19 - Projection with 100% Projection"""


def query19():
    commands = """
        CREATE TEMP TABLE query19 (
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
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(  # EXPLAIN (ANALYZE, BUFFERS)
            "INSERT INTO query19 (two, four, ten, twenty, onepercent, tenpercent, twentypercent, fiftypercent, unique3,"
            " evenonepercent, oddonepercent, stringu1, stringu2, string4) "
            "SELECT DISTINCT two, four, ten, twenty, onepercent, tenpercent, twentypercent, fiftypercent, unique3,"
            " evenonepercent, oddonepercent, stringu1, stringu2, string4 "
            "FROM cs487.tenktup1")
        conn.commit()
        cur.execute("SELECT * FROM query19")
        row = cur.fetchone()
        with open("query19.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query20 - (no index) - Minimum Aggregate Function"""


def query20():
    commands = """
        CREATE TEMP TABLE query20 (
            unique2 integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query20 "
            "SELECT MIN (cs487.tenktup1.unique2) "
            "FROM cs487.tenktup1")
        conn.commit()
        cur.execute("SELECT * FROM query20")
        row = cur.fetchone()
        with open("query20.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query21 - (no index) - Minimum Aggregate Function with 100 Partitions"""


def query21():
    commands = """
        CREATE TEMP TABLE query21 (
            unique3 integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query21 "
            "SELECT MIN (cs487.tenktup1.unique3) "
            "FROM cs487.tenktup1 "
            "GROUP BY cs487.tenktup1.onepercent")
        conn.commit()
        cur.execute("SELECT * FROM query21")
        row = cur.fetchone()
        with open("query21.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query22 - (No Index) - Sum Aggregate Function with 100 Partitions"""


def query22():
    commands = """
        CREATE TEMP TABLE query22 (
            sum integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query22 "
            "SELECT SUM (cs487.tenktup1.unique3) "
            "FROM cs487.tenktup1 "
            "GROUP BY cs487.tenktup1.onepercent")
        conn.commit()
        cur.execute("SELECT * FROM query22")
        row = cur.fetchone()
        with open("query22.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query23 - (with clustered index) - minimum Aggregate Function"""


def query23():
    commands = """
        CREATE TEMP TABLE query23 (
            unique2 integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query23 "
            "SELECT MIN (cs487.tenktup1.unique2) "
            "FROM cs487.tenktup1")
        conn.commit()
        cur.execute("SELECT * FROM query23")
        row = cur.fetchone()
        with open("query23.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query24 - (with clustered index) - Minimum Aggregate Function with 100 Partitions"""


def query24():
    commands = """
        CREATE TEMP TABLE query24 (
            min integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query24 "
            "SELECT MIN (cs487.tenktup1.unique3)"
            "FROM cs487.tenktup1 "
            "GROUP BY cs487.tenktup1.onepercent")
        conn.commit()
        cur.execute("SELECT * FROM query24")
        row = cur.fetchone()
        with open("query24.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query25 - (with clustered index) - Sum Aggregate Function with 100 Partitions"""


def query25():
    commands = """
        CREATE TEMP TABLE query25 (
            sum integer NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute(
            "INSERT INTO query25 "
            "SELECT SUM (cs487.tenktup1.unique3) "
            "FROM cs487.tenktup1 "
            "GROUP BY cs487.tenktup1.onepercent")
        conn.commit()
        cur.execute("SELECT * FROM query25")
        row = cur.fetchone()
        with open("query25.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query26 - (no indicies) - Insert 1 tuple"""


def query26():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cs487.tenktup1 "
            "VALUES(10001,10001,0,2,0,10,50,688,1950,4950,9950,1,100,"
            "'MxxxxxxxxxxxxxxxxxxxxxxxxxGxxxxxxxxxxxxxxxxxxxxxxxxC',"
            "'GxxxxxxxxxxxxxxxxxxxxxxxxxCxxxxxxxxxxxxxxxxxxxxxxxxA',"
            "'OxxxxxxxxxxxxxxxxxxxxxxxxxOxxxxxxxxxxxxxxxxxxxxxxxxO')"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique1 =10001")
        row = cur.fetchone()
        with open("query26.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query27 - (no index) - Delete 1 tuple"""


def query27():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM cs487.tenktup1 "
            "WHERE cs487.tenktup1.unique1 = 10001"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique1 >10000")
        row = cur.fetchone()
        with open("query27.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query28 - (no indicies) - Update key attribute"""


def query28():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "UPDATE cs487.tenktup1 "
            "SET unique2 = 10001 "
            "WHERE cs487.tenktup1.unique2 = 1491"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique2 = 10001")
        row = cur.fetchone()
        with open("query28.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query29 - with indicies) - Insert 1 tuple"""


def query29():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cs487.tenktup1 "
            "VALUES(10001,10002,0,2,0,10,50,688,1950,4950,9950,1,100,"
            "'MxxxxxxxxxxxxxxxxxxxxxxxxxGxxxxxxxxxxxxxxxxxxxxxxxxC',"
            "'GxxxxxxxxxxxxxxxxxxxxxxxxxCxxxxxxxxxxxxxxxxxxxxxxxxA',"
            "'OxxxxxxxxxxxxxxxxxxxxxxxxxOxxxxxxxxxxxxxxxxxxxxxxxxO')"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique1 =10001")
        row = cur.fetchone()
        with open("query29.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query30 - (with indicies) - Delete 1 tuple"""


def query30():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM cs487.tenktup1 "
            "WHERE cs487.tenktup1.unique1 = 10001"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique1 >10000")
        row = cur.fetchone()
        with open("query30.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query31 - (with indicies) - Update key attribute"""


def query31():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "UPDATE cs487.tenktup1 "
            "SET unique2 = 10001 "
            "WHERE cs487.tenktup1.unique2 = 1491"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique2 = 10001")
        row = cur.fetchone()
        with open("query31.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


"""Query32 - (with indicies) - Update indexed non-key attribute"""


def query32():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "UPDATE cs487.tenktup1 "
            "SET unique1 = 10001 "
            "WHERE cs487.tenktup1.unique1 = 1491"
        )
        conn.commit()
        cur.execute("SELECT * FROM cs487.tenktup1 WHERE cs487.tenktup1.unique1 = 1491")
        row = cur.fetchone()
        with open("query32.csv", mode='w', newline='') as query_file:
            query_writer = csv.writer(query_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            while row is not None:
                query_writer.writerow([row])
                row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def createbprime():
    """Create the TMP table to hold query results"""
    commands = """
        CREATE TABLE cs487.bprime (
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
            stringu1 character(52) NOT NULL,
            stringu2 character(52) NOT NULL,
            string4 character(52) NOT NULL
        )
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(commands)
        cur.execute("INSERT INTO cs487.bprime SELECT * FROM cs487.tenktup2 WHERE cs487.tenktup2.unique2 < 1000")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def connect():
    """Connect to the PostgreSQL database Server"""
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        cur.execute("SELECT unique2, stringu2 FROM cs487.onektup")
        row = cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    # connect()
    # createtmp()
    # createbprime()
    # query1()
    # query2()
    # query3()
    # query4()
    # query5()
    # query6()
    # query7()
    # query8()
    # query9()
    # query10()
    query11()
# query12()
# query13()
# query14()
# query15()
# query16()
# query17()
# query18()
# query19()
# query20()
# query21()
# query22()
# query23()
# query24()
# query25()
# query26()
# query27()
# query28()
# query29()
# query30()
# query31()
# query32()
