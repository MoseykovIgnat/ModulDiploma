#!/usr/bin/python
import MySQLdb
import SQLParser.xxxdbrc
import time


def create_connect_to_db(config):
    connection = MySQLdb.connect(host=config["hostname"],
                                 user=config["username"],
                                 passwd=config["password"],
                                 db=config["dbname"],
                                 port=config["port"],
                                 charset='utf8')

    return connection


class Getters(object):
    def __init__(self, interval_time, source):
        self.interval_time = interval_time  # example for 60 sec = 20
        self.source = source
        self.expired_time = 0

    def update_time(self, load_time):
        self.expired_time += load_time





class Db_getter(Getters):
    def __init__(self, interval_time, source):
        super(Db_getter, self).__init__(interval_time, source)
        self.config = SQLParser.xxxdbrc.config(source)
        self.connection = create_connect_to_db(self.config)
        self.cursor = self.connection.cursor()

    def db_close_connection(self):
        self.cursor.close()
        self.connection.close()


class Clb_db_getter(Db_getter):
    def __init__(self):
        super(Clb_db_getter, self).__init__(10, 'clb')

    def cld_db_print(self):
        if self.interval_time <= self.expired_time:
            print("Cld_worked (it's working every 10 sec)")
            self.expired_time = 0



class Adm_db_getter(Db_getter):
    def __init__(self):
        super(Adm_db_getter, self).__init__(5, 'adm')

    def adm_db_print(self):
        if self.interval_time <= self.expired_time:
            print("adm_worked (it's working every 5 sec)")
            self.expired_time = 0


def create_connection_djangodb():
    connection = MySQLdb.connect(host='-00.',
                                 user='dsf',
                                 passwd='sdfg',
                                 db='dsfg',
                                 port=123,
                                 charset='utf8')
    return connection


def close_cursor_connection(cursor, connection):
    cursor.close()
    connection.close()
    print("Connection was closed")


djangodb_connection = create_connection_djangodb()
djangodb_cursor = djangodb_connection.cursor()

clbdb = Clb_db_getter()
admdb = Adm_db_getter()

while True:
    current_time = time.time()
    clbdb.cld_db_print()
    admdb.adm_db_print()
    time.sleep(5 - ((time.time() - current_time) % 60.0))
    lead_time = time.time() - current_time
    clbdb.update_time(lead_time)
    admdb.update_time(lead_time)

close_cursor_connection(djangodb_cursor, djangodb_connection)
clbdb.db_close_connection()
admdb.db_close_connection()
print("everything is working!")
