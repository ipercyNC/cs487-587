#!/usr/bin/python
from configparser import ConfigParser



def pgSQLconfig(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def mySQLconfig(filename='database.ini', section='mysql'):
    #createa a parser
    parser = ConfigParser()
    #read config file
    parser.read(filename)

    #get section, default to mysql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]]=param[1]
    else:
        raise Exception('Section {0} not found in teh {1} file'.format(section, filename))

    return db