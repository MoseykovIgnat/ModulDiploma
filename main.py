#!/usr/bin/python
import MySQLdb
import SQLParser.xxxdbrc


def create_connect_to_db(config):
    connection = MySQLdb.connect(host=config["hostname"],
                                 user=config["username"],
                                 passwd=config["password"],
                                 db=config["dbname"],
                                 port=config["port"],
                                 charset='utf8')

    return connection


class Getters(object):
    def __init__(self, time, source):
        self.time = time
        self.source = source


class Db_getter(Getters):
    def __init__(self, time, source):
        super(Getters,self).__init__(time, source)
        self.config = SQLParser.xxxdbrc.config(source)
        self.connection = create_connect_to_db(self.config)
        self.cursor = self.connection.cursor()

    def hello(self):
        print("all is right")


clbdb = Db_getter(5, 'clb')
clbdb.hello()
