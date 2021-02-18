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
        self.time_5sec = time  # example for 60 sec = 20
        self.source = source


class Db_getter(Getters):
    def __init__(self, time, source):
        super(Getters, self).__init__(time, source)
        self.config = SQLParser.xxxdbrc.config(source)
        self.connection = create_connect_to_db(self.config)
        self.cursor = self.connection.cursor()

    def db_close_connection(self):
        self.cursor.close()
        self.connection.close()

    def hello(self):
        print("all is right")






def close_cursor_connection(cursor, connection):
    cursor.close()
    connection.close()
    print("Connection was closed")


# Создаем одинарное подключение для БД Django
djangodb_connection = create_connection_djangodb()
djangodb_cursor = djangodb_connection.cursor()

# Основная работа
clbdb = Db_getter(5, 'clb')
clbdb.hello()
clbdb.db_close_connection()

# Закрываем соединение в конце работы пограммы
close_cursor_connection(djangodb_cursor, djangodb_connection)