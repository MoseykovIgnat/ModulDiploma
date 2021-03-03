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


def getter_for_lastinteger_lastdouble(cursor, channel_id, channel_type):
    if channel_type == 0:
        cursor.execute("select c_value from t_lastinteger where c_channel=%s" % channel_id)
    if channel_type == 1:
        cursor.execute("select c_value from t_lastdouble where c_channel=%s" % channel_id)
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
                config = SQLParser.xxxdbrc.config(name)
                connection = create_connect_to_db(config)
                cursor = connection.cursor(MySQLdb.cursors.DictCursor)
                self.data[name] = Connection_db(name, config, connection, cursor)
                return self.data[name]
            except:
                print("I can't connect to db: " + name)
        else:
            return self.data[name]

    def refresh_dict(self):
        for c in self.data.values():
            if not c.connection:
                try:
                    c.config = SQLParser.xxxdbrc.config(c.name)
                    c.connection = create_connect_to_db(c.config)
                    c.cursor = c.connection.cursor(MySQLdb.cursors.DictCursor)
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
        super(Setter_django, self).__init__(5, 'sc', db_manager)

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

'''Поиск всех id каналов для всех Path из sc_paths with Online status'''


class Channel_id_and_type(object):
    def __init__(self, channel_id, channel_type):
        self.channel_id = channel_id
        self.channel_type = channel_type


def get_path_parent_tem(path):
    return path[::-1].split('/', 1)[-1][::-1]


def get_path_name_tem(path):
    return path.split('/')[-1]

# class Channels_tem_db_getter(Db_getter_setter):
#     def __init__(self, db_manager):
#         super(Channels_tem_db_getter, self).__init__(3600, 'tem', db_manager)
#         self.channels_tem_db = dict()
#
#     def get_channels_tem_db(self):
#         return self.channels_tem_db
#
#     def getter(self, alias): #
#         res = dict()
#         query_for_channels = "select * from v_dir where parent=%s and name = %s"
#         for key, value in alias.iteritems():
#             a = value[::-1]
#             a = a.split('/', 1)[-1]
#             args = (a[::-1], value.split('/')[-1])
#             self.db.cursor.execute(query_for_channels, args)
#             data = self.db.cursor.fetchall()
#             for a in data:
#                 res[key] = Channel_id_and_type(a['id'], a['type'])
#         self.channels_tem_db = res
# class DC1_tem_db_getter(Db_getter_setter):
#     def __init__(self, db_manager):
#         super(DC1_tem_db_getter, self).__init__(5, 'tem', db_manager)
#
#     def getter(self, channel_type, res_dict):
#         if self.interval_time <= self.expired_time:
#             try:
#                 res_dict['DC1'] = getter_for_lastinteger_lastdouble(self.db.cursor, channel_type)
#                 print("Got DC1")
#                 self.expired_time = 0
#             except:
#                 print('Attempt to get DC1 failed')
# class DC1inc_tem_db_getter(Db_getter_setter):
#     def __init__(self, db_manager):
#         super(DC1inc_tem_db_getter, self).__init__(10, 'tem', db_manager)
#
#     def getter(self, channel_type, res_dict):
#         if self.interval_time <= self.expired_time:
#             try:
#                 res_dict['DC1inc'] = getter_for_lastinteger_lastdouble(self.db.cursor, channel_type)
#                 print("Got DC1inc")
#                 self.expired_time = 0
#             except:
#                 print('Attempt to get DC1inc failed')
# '''Словарь alias для temdbase'''
# alias_for_tem_db = {'LOCK': '/VEPP2K/STATUS/SND_INTERLOCK', 'FLT': '/SND/SCALERS/INTEGRALS/FLT',
#                     'FLTinc': '/SND/SCALERS/INCREMENTS/FLT', 'ST': '/SND/SCALERS/INTEGRALS/ST', 'E_laser': '/EMS/E',
#                     'dE_laser': '/EMS/DE', 'BEP_PMT': '/VEPP2K/STATUS/BEP_PMT', 'STinc': '/SND/SCALERS/INCREMENTS/ST',
#                     'shunt': '/VEPP2K/STATUS/VEPP_POWER_CURRENT', 'E_VEPP': '/VEPP2K/STATUS/VEPP_ENERGY',
#                     'E_NMR': '/VEPP2K/STATUS/E_NMR', 'L': '/SND/DERIVED/L', 'IL': '/SND/DERIVED/IL',
#                     'IProd': '/SND/DERIVED/IProd', 'DC1': '/SND/SCALERS/INTEGRALS/DC1',
#                     'DC1inc': '/SND/SCALERS/INCREMENTS/DC1', 'time': '/SND/SCALERS/INTEGRALS/TIME',
#                     'Tlive': '/SND/SCALERS/INTEGRALS/TIMELIVE', 'Tliveinc': '/SND/SCALERS/INCREMENTS/TIMELIVE',
#                     'E_PMT': '/VEPP2K/STATUS/VEPP_IE', 'P_PMT': '/VEPP2K/STATUS/VEPP_IP',
#                     'VEPP_FZ': '/VEPP2K/STATUS/VEPP_FZ', 'run': '/SND/SCALERS/RUN',
#                     'timeinc': '/SND/SCALERS/INCREMENTS/TIME', 'FLT1': '/SND/SCALERS/INTEGRALS/FLT1',
#                     'FLT1inc': '/SND/SCALERS/INCREMENTS/FLT1', 'etau': '/VEPP2K/STATUS/VEPP_E_TAU',
#                     'ptau': '/VEPP2K/STATUS/VEPP_P_TAU', 'fztau': '/VEPP2K/STATUS/VEPP_FZ_TAU',
#                     'ePMT': '/VEPP2K/CAS/VEPP/CURRENTS/EPMT', 'pPMT': '/VEPP2K/CAS/VEPP/CURRENTS/PPMT',
#                     'E0': '/VEPP2K/STATUS/VEPP_E0', 'setE': '/VEPP2K/STATUS/VEPP_SET_ENERGY',
#                     'ILSH': '/SND/DERIVED/ILSH', 'ILPT': '/SND/DERIVED/ILPT', 'k': '/SND/DERIVED/PWRFLT1',
#                     'sigma0': '/SND/DERIVED/SIGMAFLT1', 'fcosm': '/SND/DERIVED/CSMFLT1',
#                     'RMNAFLT': '/SND/DERIVED/RMNAFLT', 'E_EMS_DT': '/EMS/DT', 'Tklk': '/SND/CPS/KAS/1/007',
#                     'NMR_1M1': '/VEPP2K/STATUS/NMR_1M1', 'NMR_1M2': '/VEPP2K/STATUS/NMR_1M2',
#                     'NMR_2M1': '/VEPP2K/STATUS/NMR_2M1', 'NMR_2M2': '/VEPP2K/STATUS/NMR_2M2',
#                     'NMR_3M1': '/VEPP2K/STATUS/NMR_3M1', 'NMR_3M2': '/VEPP2K/STATUS/NMR_3M2',
#                     'NMR_4M1': '/VEPP2K/STATUS/NMR_4M1', 'NMR_4M2': '/VEPP2K/STATUS/NMR_4M2',
#                     'NMR_AVG': '/VEPP2K/STATUS/NMR_AVG', 'VEPP_RF_FREQ': '/VEPP2K/CAS/VEPP/RF/FREQ',
#                     'GENC': '/SND/SCALERS/INTEGRALS/GENC',
#                     'GENCinc': '/SND/SCALERS/INCREMENTS/GENC'}  # Создали словарь alias для путей (parent)


class Tem_db_getter(Db_getter_setter):
    def __init__(self, time_interval, db_manager, path):
        super(Tem_db_getter, self).__init__(time_interval, 'tem', db_manager)
        self.path = path

    def getter(self, res_dict):
        if self.interval_time <= self.expired_time:
            try:
                query_for_channel = "select * from v_dir where parent=%s and name = %s"
                args_for_channel = (get_path_parent_tem(self.path), get_path_name_tem(self.path))
                self.db.cursor.execute(query_for_channel, args_for_channel)
                data = self.db.cursor.fetchone()
                res_dict[self.path] = getter_for_lastinteger_lastdouble(self.db.cursor, data['id'], data['type'])
                print("Get " + get_path_name_tem(self.path))
            except:
                print('Attempt to get variable failed')


#Функция для создания массива гетеров!
def create_getters_for_tem(django_db_connection, getter_list, db_manager):
    django_db_connection.db.cursor.execute('SELECT s1.path,s1.interval_time FROM sc_paths_online s1 LEFT JOIN sc_paths_online s2 ON s1.path = s2.path AND s2.interval_time < s1.interval_time WHERE s2.path_id IS NULL')
    data = django_db_connection.db.cursor.fetchall()
    for a in data:
        getter_list.append(Tem_db_getter(a['interval_time'], db_manager, a['path']))
        print(a['path'])
    print('Все нужные гетеры созданы')


def execute_getters_for_tem(res_dict, getters):
    for tem_getter in getters:
        tem_getter.getter(res_dict)


def update_getters_time_for_tem(getters, update_time):
    for tem_getter in getters:
        tem_getter.update_time(update_time)


manager_of_db = Manager()  # создали менеджера db
setter_django = Setter_django(manager_of_db)    # Создали сеттер
# tem_ch = Channels_tem_db_getter(manager_of_db)
# tem_ch.getter(alias_for_tem_db)  # метод для поиска всех id для temdbase
results = dict()  # словарь для результатов (далее с его помощью будем заполнять БД django)
tem_getters = list() # Лист для всех гетеров с tem
# clbdb = Clb_db_getter(manager_of_db, 10)
# admdb = Adm_db_getter(manager_of_db, 5)
# test = test_db_getter(manager_of_db,5)
# DC1_getter = DC1_tem_db_getter(manager_of_db)
# DC1inc_getter = DC1inc_tem_db_getter(manager_of_db)  # Тест, что подключение только одно

create_getters_for_tem(setter_django, tem_getters, manager_of_db)
time_checker = 0
while True:
    current_time = time.time()
    execute_getters_for_tem(results,tem_getters)
    setter_django.input_in_django_db(results)
    time.sleep(5 - (time.time() - current_time))
    time_checker += 5
    print("Прошло времени: " + str(time_checker) + 'сек')
    lead_time = time.time() - current_time
    # DC1_getter.update_time(lead_time)
    # DC1inc_getter.update_time(lead_time)
    update_getters_time_for_tem(tem_getters,lead_time)
    setter_django.update_time(lead_time)
    # clbdb.update_time(lead_time)
    # admdb.update_time(lead_time)

# close_cursor_connection(djangodb_cursor, djangodb_connection)
# clbdb.db_close_connection()
# admdb.db_close_connection()
print("everything is working!")
