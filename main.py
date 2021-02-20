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


def close_cursor_connection(cursor, connection):
    cursor.close()
    connection.close()
    print("Connection was closed")


class Connection_db(object):
    def __init__(self, db_name, config, connection, cursor):
        self.db_name = db_name
        self.config = config
        self.connection = connection
        self.cursor = cursor


class Manager(object):
    def __init__(self):
        self.data = dict()

    def get_db_con(self, name):
        if name not in self.data:
            try:
                config = SQLParser.xxxdbrc.config(name)
                connection = create_connect_to_db(config)
                cursor = connection.cursor()
                self.data[name] = Connection_db(name, config, connection, cursor)
                return self.data[name]
            except:
                print("I can't connect to db: " + name)


    def refresh_dict(self):
        for c in self.data.values():
            if not c.connection:
                try:
                    c.config = SQLParser.xxxdbrc.config(c.name)
                    c.connection = create_connect_to_db(c.config)
                    c.cursor = c.connection.cursor()
                except:
                    print("I can't connect to db" + c.name)


class Getters(object):
    def __init__(self, interval_time, source):
        self.interval_time = interval_time  # example for 60 sec = 20
        self.source = source
        self.expired_time = 0

    def update_time(self, load_time):
        self.expired_time += load_time


class Db_getter(Getters):
    def __init__(self, interval_time, source, db_manager):
        super(Db_getter, self).__init__(interval_time, source)
        self.db = Manager.get_db_con(db_manager, source)

    def db_close_connection(self):
        self.db.cursor.close()
        self.db.connection.close()


class Clb_db_getter(Db_getter):
    def __init__(self, db_manager):
        super(Clb_db_getter, self).__init__(10, 'clb', db_manager)

    def cld_db_print(self):
        if self.interval_time <= self.expired_time:
            print("Cld_worked (it's working every 10 sec)")
            self.expired_time = 0


class Adm_db_getter(Db_getter):
    def __init__(self, db_manager):
        super(Adm_db_getter, self).__init__(5, 'adm', db_manager)

    def adm_db_print(self):
        if self.interval_time <= self.expired_time:
            print("adm_worked (it's working every 5 sec)")
            self.expired_time = 0


class test_db_getter(Db_getter):
    def __init__(self, db_manager):
        super(test_db_getter, self).__init__(5, 'asdg', db_manager)

def create_connection_djangodb():
    connection = MySQLdb.connect(host='asdf-00.asdg',
                                 user='asdg',
                                 passwd='asdg',
                                 db='asdg',
                                 port=3306,
                                 charset='asdg')
    return connection


djangodb_connection = create_connection_djangodb()
djangodb_cursor = djangodb_connection.cursor()

manager_of_db = Manager()
clbdb = Clb_db_getter(manager_of_db)
admdb = Adm_db_getter(manager_of_db)
test = test_db_getter(manager_of_db)

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
