#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import SQLParser.xxxdbrc
import time

'''Возможность взять alias для temdbase from txt.file'''


def create_alias_to_tem_db():
    d = dict()
    inp = open("Alias.txt", 'r')
    for i in inp.readlines():
        key, val = i.strip().split(' ')
        d[key] = val
    return d


def create_connect_to_db(config):
    connection = MySQLdb.connect(host=config["hostname"],
                                 user=config["username"],
                                 passwd=config["password"],
                                 db=config["dbname"],
                                 port=config["port"],
                                 charset='utf8')

    return connection


def create_connection_djangodb():
    connection = MySQLdb.connect(host='sndfarm08-00.sndonline',
                                 user='scadmin',
                                 passwd='sc1s1mp0rtnt',
                                 db='sctestdb',
                                 port=3306,
                                 charset='utf8')
    return connection


def close_cursor_connection(cursor, connection):
    cursor.close()
    connection.close()
    print("Connection was closed")


def getter_for_lastinteger_lastdouble(cursor, channel):
    if channel.channel_type == 0:
        cursor.execute("select c_value from t_lastinteger where c_channel=%s" % channel.channel_id)
    if channel.channel_type == 1:
        cursor.execute("select c_value from t_lastdouble where c_channel=%s" % channel.channel_id)
    data = cursor.fetchall()
    for x in data:
        res = x['c_value']
    return res


def getter_for_lastdouble(cursor,channel):
    cursor.execute("select c_value from t_lastdouble where c_channel=%s" % channel)
    data = cursor.fetchall()
    for x in data:
        res = x['c_value']
    return res


class Connection_db(object):
    def __init__(self, db_name, config, connection, cursor):
        self.db_name = db_name
        self.config = config
        self.connection = connection
        self.cursor = cursor


'''Менеджер для поддержания и создания единственного подключения к каждой БД'''


class Manager(object):
    def __init__(self):
        self.data = dict()

    def get_db_con(self, name):
        if name not in self.data:
            try:
                if name != 'django_db':
                    config = SQLParser.xxxdbrc.config(name)
                    connection = create_connect_to_db(config)
                    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
                    self.data[name] = Connection_db(name, config, connection, cursor)
                    return self.data[name]
                if name == 'django_db':
                    connection_django = create_connection_djangodb()
                    cursor_django = connection_django.cursor(MySQLdb.cursors.DictCursor)
                    self.data[name] = Connection_db(name, 0, connection_django, cursor_django)
                    return self.data[name]
            except:
                print("I can't connect to db: " + name)
        else:
            return self.data[name]

    def refresh_dict(self):
        for c in self.data.values():
            if not c.connection:
                try:
                    if c.name != 'django_db':
                        c.config = SQLParser.xxxdbrc.config(c.name)
                        c.connection = create_connect_to_db(c.config)
                        c.cursor = c.connection.cursor(MySQLdb.cursors.DictCursor)
                    else:
                        c.connection = create_connection_djangodb()
                        c.cursor = c.connection.cursor(MySQLdb.cursors.DictCursor)
                        print("Reconnected to django db")
                except:
                    print("I can't connect to db" + c.name)


class Getter_setter(object):
    def __init__(self, interval_time, source):
        self.interval_time = interval_time  # example for 60 sec = 20
        self.source = source
        self.expired_time = 0

    def update_time(self, load_time):
        self.expired_time += load_time


class Db_getter_setter(Getter_setter):
    def __init__(self, interval_time, source, db_manager):
        super(Db_getter_setter, self).__init__(interval_time, source)
        self.db = Manager.get_db_con(db_manager, source)

    def db_close_connection(self):
        self.db.cursor.close()
        self.db.connection.close()


'''Сеттер'''


class Setter_django(Db_getter_setter):
    def __init__(self, db_manager):
        super(Setter_django, self).__init__(5, 'django_db', db_manager)

    def input_in_django_db(self, result):
        if self.interval_time <= self.expired_time:
            if len(result) > 0:
                for key, value in result.iteritems():
                    query ="replace into scresults_test(var_title, value, submission_date) values(%s, %s, NOW())"
                    self.db.cursor.execute(query, (key, value))
                    self.db.connection.commit()
                    print('В таблице обнавлено: Name '+str(key)+' ; Value '+str(value))
            else:
                print("Словарь пуст")


'''Геттеры с других БД на будущее'''
# class Clb_db_getter(Db_getter_setter):
#     def __init__(self, db_manager, interval):
#         super(Clb_db_getter, self).__init__(interval, 'clb', db_manager)
#
#     def cld_db_print(self):
#         if self.interval_time <= self.expired_time:
#             print("Cld_worked (it's working every 10 sec)")
#             self.expired_time = 0
#
#
# class Adm_db_getter(Db_getter_setter):
#     def __init__(self, db_manager, interval):
#         super(Adm_db_getter, self).__init__(interval, 'adm', db_manager)
#
#     def adm_db_print(self):
#         if self.interval_time <= self.expired_time:
#             print("adm_worked (it's working every 5 sec)")
#             self.expired_time = 0


'''Поиск всех id каналов для всех aliases'''


class Channel_id_and_type(object):
    def __init__(self, channel_id, channel_type):
        self.channel_id = channel_id
        self.channel_type = channel_type


class Channels_tem_db_getter(Db_getter_setter):
    def __init__(self, db_manager):
        super(Channels_tem_db_getter, self).__init__(3600, 'tem', db_manager)
        self.channels_tem_db = dict()

    def get_channels_tem_db(self):
        return self.channels_tem_db

    def getter(self, alias):
        res = dict()
        query = "select * from v_dir where parent=%s and name = %s"
        for key, value in alias.iteritems():
            a = value[::-1]
            a = a.split('/', 1)[-1]
            args = (a[::-1], value.split('/')[-1])
            self.db.cursor.execute(query, args)
            data = self.db.cursor.fetchall()
            for a in data:
                res[key] = Channel_id_and_type(a['id'], a['type'])
        self.channels_tem_db = res


'''Поиск переменной DC1'''


class DC1_tem_db_getter(Db_getter_setter):
    def __init__(self, db_manager):
        super(DC1_tem_db_getter, self).__init__(5, 'tem', db_manager)

    def getter(self, channel_type, res_dict):
        if self.interval_time <= self.expired_time:
            try:
                res_dict['DC1'] = getter_for_lastinteger_lastdouble(self.db.cursor, channel_type)
                print("Got DC1")
                self.expired_time = 0
            except:
                print('Attempt to get DC1 failed')



# '''Создаем подключение к БД DJNAGO для сеттеров'''
# djangodb_connection = create_connection_djangodb()
# djangodb_cursor = djangodb_connection.cursor()
'''Словарь alias для temdbase'''
alias_for_tem_db = {'LOCK': '/VEPP2K/STATUS/SND_INTERLOCK', 'FLT': '/SND/SCALERS/INTEGRALS/FLT',
                    'FLTinc': '/SND/SCALERS/INCREMENTS/FLT', 'ST': '/SND/SCALERS/INTEGRALS/ST', 'E_laser': '/EMS/E',
                    'dE_laser': '/EMS/DE', 'BEP_PMT': '/VEPP2K/STATUS/BEP_PMT', 'STinc': '/SND/SCALERS/INCREMENTS/ST',
                    'shunt': '/VEPP2K/STATUS/VEPP_POWER_CURRENT', 'E_VEPP': '/VEPP2K/STATUS/VEPP_ENERGY',
                    'E_NMR': '/VEPP2K/STATUS/E_NMR', 'L': '/SND/DERIVED/L', 'IL': '/SND/DERIVED/IL',
                    'IProd': '/SND/DERIVED/IProd', 'DC1': '/SND/SCALERS/INTEGRALS/DC1',
                    'DC1inc': '/SND/SCALERS/INCREMENTS/DC1', 'time': '/SND/SCALERS/INTEGRALS/TIME',
                    'Tlive': '/SND/SCALERS/INTEGRALS/TIMELIVE', 'Tliveinc': '/SND/SCALERS/INCREMENTS/TIMELIVE',
                    'E_PMT': '/VEPP2K/STATUS/VEPP_IE', 'P_PMT': '/VEPP2K/STATUS/VEPP_IP',
                    'VEPP_FZ': '/VEPP2K/STATUS/VEPP_FZ', 'run': '/SND/SCALERS/RUN',
                    'timeinc': '/SND/SCALERS/INCREMENTS/TIME', 'FLT1': '/SND/SCALERS/INTEGRALS/FLT1',
                    'FLT1inc': '/SND/SCALERS/INCREMENTS/FLT1', 'etau': '/VEPP2K/STATUS/VEPP_E_TAU',
                    'ptau': '/VEPP2K/STATUS/VEPP_P_TAU', 'fztau': '/VEPP2K/STATUS/VEPP_FZ_TAU',
                    'ePMT': '/VEPP2K/CAS/VEPP/CURRENTS/EPMT', 'pPMT': '/VEPP2K/CAS/VEPP/CURRENTS/PPMT',
                    'E0': '/VEPP2K/STATUS/VEPP_E0', 'setE': '/VEPP2K/STATUS/VEPP_SET_ENERGY',
                    'ILSH': '/SND/DERIVED/ILSH', 'ILPT': '/SND/DERIVED/ILPT', 'k': '/SND/DERIVED/PWRFLT1',
                    'sigma0': '/SND/DERIVED/SIGMAFLT1', 'fcosm': '/SND/DERIVED/CSMFLT1',
                    'RMNAFLT': '/SND/DERIVED/RMNAFLT', 'E_EMS_DT': '/EMS/DT', 'Tklk': '/SND/CPS/KAS/1/007',
                    'NMR_1M1': '/VEPP2K/STATUS/NMR_1M1', 'NMR_1M2': '/VEPP2K/STATUS/NMR_1M2',
                    'NMR_2M1': '/VEPP2K/STATUS/NMR_2M1', 'NMR_2M2': '/VEPP2K/STATUS/NMR_2M2',
                    'NMR_3M1': '/VEPP2K/STATUS/NMR_3M1', 'NMR_3M2': '/VEPP2K/STATUS/NMR_3M2',
                    'NMR_4M1': '/VEPP2K/STATUS/NMR_4M1', 'NMR_4M2': '/VEPP2K/STATUS/NMR_4M2',
                    'NMR_AVG': '/VEPP2K/STATUS/NMR_AVG', 'VEPP_RF_FREQ': '/VEPP2K/CAS/VEPP/RF/FREQ',
                    'GENC': '/SND/SCALERS/INTEGRALS/GENC',
                    'GENCinc': '/SND/SCALERS/INCREMENTS/GENC'}  # Создали словарь alias для путей (parent)
manager_of_db = Manager()  # создали менеджера db
setter_django = Setter_django(manager_of_db)    # Создали сеттер
tem_ch = Channels_tem_db_getter(manager_of_db)
tem_ch.getter(alias_for_tem_db)  # метод для поиска всех id для temdbase
results = dict()  # словарь для результатов (далее с его помощью будем заполнять БД django)

# clbdb = Clb_db_getter(manager_of_db, 10)
# admdb = Adm_db_getter(manager_of_db, 5)
# test = test_db_getter(manager_of_db,5)
DC1_getter = DC1_tem_db_getter(manager_of_db)
DC1_getter2 = DC1_tem_db_getter(manager_of_db)  # Тест, что подключение только одно

while True:
    current_time = time.time()
    # clbdb.cld_db_print()
    # admdb.adm_db_print()
    DC1_getter.getter(tem_ch.get_channels_tem_db()['DC1'], results)
    setter_django.input_in_django_db(results)
    time.sleep(5 - ((time.time() - current_time) % 60.0))
    lead_time = time.time() - current_time
    DC1_getter.update_time(lead_time)
    setter_django.update_time(lead_time)
    # clbdb.update_time(lead_time)
    # admdb.update_time(lead_time)

# close_cursor_connection(djangodb_cursor, djangodb_connection)
# clbdb.db_close_connection()
# admdb.db_close_connection()
print("everything is working!")
