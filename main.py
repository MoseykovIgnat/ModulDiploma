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
    def __init__(self, quantum_5sec, source):
        self.quantum_5sec = quantum_5sec  # example for 60 sec = 20
        self.source = source
        self.quantum_current = 0

    def get_quantum_5sec(self):
        return self.quantum_5sec

    def get_quantum_current(self):
        return self.quantum_current

    def set_quantum_start(self, new_amount):
        self.quantum_current = new_amount

    def increment_quantum_current(self):
        self.quantum_current += 1


class Db_getter(Getters):
    def __init__(self, quantum_5sec, source):
        super(Db_getter, self).__init__(quantum_5sec, source)
        self.config = SQLParser.xxxdbrc.config(source)
        self.connection = create_connect_to_db(self.config)
        self.cursor = self.connection.cursor()

    def db_close_connection(self):
        self.cursor.close()
        self.connection.close()


class Clb_db_getter(Db_getter):
    def __init__(self):
        super(Clb_db_getter, self).__init__(2, 'clb')

    def cld_db_print(self):
        print("Cld_worked (it's working every 10 sec)")


class Adm_db_getter(Db_getter):
    def __init__(self):
        super(Adm_db_getter, self).__init__(1, 'adm')

    def adm_db_print(self):
        print("adm_worked (it's working every 5 sec)")


def create_connection_djangodb():
    connection = MySQLdb.connect(host='123',
                                 user='21',
                                 passwd='1',
                                 db='3',
                                 port=31306,
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
    if clbdb.get_quantum_current() == clbdb.get_quantum_5sec():
        clbdb.cld_db_print()
        clbdb.set_quantum_start(0)
        break
    if admdb.get_quantum_current() == admdb.get_quantum_5sec():
        admdb.adm_db_print()
        admdb.set_quantum_start(0)
    clbdb.increment_quantum_current()
    admdb.increment_quantum_current()
    time.sleep(5 - ((time.time() - current_time) % 60.0))

close_cursor_connection(djangodb_cursor, djangodb_connection)
clbdb.db_close_connection()
admdb.db_close_connection()
print("everything is working!")
