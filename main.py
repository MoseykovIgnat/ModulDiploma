#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import SQLParser.xxxdbrc
import time
import re

dict_for_min_max_type = {'sec': 'second', 'min': 'minute', 'h': 'hour', 'd': 'day', 'm': 'month', 'y': 'year'}


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


# ----Get different info about variable
def get_source_of_var(var_info):
    return var_info.split(':')[0]


def get_path_of_var(var_info):
    return var_info.split(':')[1]


def get_param_of_var(var_info):
    return var_info.split(':')[2]


# -------------------------


def getter_for_lastinteger_lastdouble_value(cursor, channel_id, channel_type):
    if channel_type == 0:
        cursor.execute("select c_value from t_lastinteger where c_channel=%s" % channel_id)
    if channel_type == 1:
        cursor.execute("select c_value from t_lastdouble where c_channel=%s" % channel_id)
    data = cursor.fetchall()
    for x in data:
        res = x['c_value']
    return {'result': res, 'need_to_save_to_django': 1}


def getter_for_lastinteger_lastdouble_fvalue(cursor, channel_id_value, channel_type_value, channel_id_timealive):
    if channel_type_value == 0:
        query = "select (select c_value from t_lastinteger where c_channel=%s)/(select c_value from t_lastdouble where c_channel=%s) as Hz"

    if channel_type_value == 1:
        query = "select (select c_value from t_lastdouble where c_channel=%s)/(select c_value from t_lastdouble where c_channel=%s) as Hz"
    cursor.execute(query, (channel_id_value, channel_id_timealive['id']))
    data = cursor.fetchall()
    for x in data:
        res = x['Hz']
    return {'result': res, 'need_to_save_to_django': 1}


def getter_for_lastinteger_lastdouble_stamp(cursor, channel_id, channel_type):
    if channel_type == 0:
        cursor.execute("select c_stamp from t_lastinteger where c_channel=%s" % channel_id)
    if channel_type == 1:
        cursor.execute("select c_stamp from t_lastdouble where c_channel=%s" % channel_id)
    data = cursor.fetchall()
    for x in data:
        res = x['c_stamp']
    return {'result': res, 'need_to_save_to_django': 1}


def getter_for_integer_double_prev(cursor, channel_id, channel_type):
    if channel_type == 0:
        cursor.execute(
            "select c_value from t_datainteger where c_channel=%s order by c_stamp DESC limit 1,1" % channel_id)
    if channel_type == 1:
        cursor.execute(
            "select c_value from t_datadouble where c_channel=%s order by c_stamp DESC limit 1,1" % channel_id)
    data = cursor.fetchall()
    for x in data:
        res = x['c_value']
    return {'result': res, 'need_to_save_to_django': 1}


def getter_for_integer_double_prev_stamp(cursor, channel_id, channel_type):
    if channel_type == 0:
        cursor.execute(
            "select c_stamp from t_datainteger where c_channel=%s order by c_stamp DESC limit 1,1" % channel_id)
    if channel_type == 1:
        cursor.execute(
            "select c_stamp from t_datadouble where c_channel=%s order by c_stamp DESC limit 1,1" % channel_id)
    data = cursor.fetchall()
    for x in data:
        res = x['c_stamp']
    return {'result': res, 'need_to_save_to_django': 1}


def getter_for_integer_double_sample(cursor, channel_id, channel_type, sample_type):
    # sec,min,hour,d,m,y
    # sample_type = 'sum(-100d,-10d)' -> min=100d; max=10d; min_val=100; min_type='d',res_min = 10 days; max_val=10; max_type='d';
    print(sample_type)
    interval = sample_type.split('(')[1].replace(')', '').replace('-', '').split(',')
    min_res = interval[0]
    min_type = "".join(re.findall(r'\D', min_res))
    min_val = min_res.replace(min_type, '')
    max_res = interval[1]
    max_type = "".join(re.findall(r'\D', max_res))
    max_val = max_res.replace(max_type, '')
    sample_type_name = sample_type.split('(')[0]
    sql = 'select c_value from table_name where c_channel=%s and c_stamp > round(unix_timestamp(date_sub(now(),interval min_val min_type))*1000000) and c_stamp < round(unix_timestamp(date_sub(now(),interval max_val max_type))*1000000)'
    sql = sql.replace('min_val', min_val).replace('min_type', dict_for_min_max_type[min_type]).replace('max_val',
                                                                                                       max_val).replace(
        'max_type', dict_for_min_max_type[max_type])
    if channel_type == 0:
        sql = sql.replace('table_name', 't_datainteger')
    if channel_type == 1:
        sql = sql.replace('table_name', 't_datadouble')
    cursor.execute(sql, channel_id)
    data = cursor.fetchall()
    if not data:
        return {'result': 'Empty', 'need_to_save_to_django': 1}
    if sample_type_name == 'avg':
        counter = 0
        res = 0
        for x in data:
            res = res + float(x['c_value'])
            counter = counter + 1
        return {'result': res / counter, 'need_to_save_to_django': 1}
    if sample_type_name == 'min':
        res_min = float(data[0]['c_value'])
        for x in data:
            if res_min > float(x['c_value']):
                res_min = float(x['c_value'])
        return {'result': res_min, 'need_to_save_to_django': 1}
    if sample_type_name == 'max':
        res_max = float(data[0]['c_value'])
        for x in data:
            if res_max < float(x['c_value']):
                res_max = float(x['c_value'])
        return {'result': res_max, 'need_to_save_to_django': 1}
    if sample_type_name == 'sum':
        res = 0
        for x in data:
            res = res + float(x['c_value'])
        return {'result': res, 'need_to_save_to_django': 1}


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
                    if value['need_to_save_to_django'] == 1:
                        query = "replace into scresults_test(var_title, value, submission_date) values(%s, %s, NOW())"
                        query_with_empty_res = "replace into scresults_test(var_title, comment, value, submission_date) values(%s, %s, 0, NOW())"
                        if value['result'] == 'Empty':
                            self.db.cursor.execute(query_with_empty_res, (key, value['result']))
                        else:
                            self.db.cursor.execute(query, (key, value['result']))
                        self.db.connection.commit()
                        print('В таблице обнавлено: Name: ' + str(key) + ' ; Value:' + str(value['result']))
                        value['need_to_save_to_django'] = 0
            else:
                print("Нет новых параметров для сохранения")

    def delete_overdue_data(self):
        delete_overdue_data_time = self.interval_time * 2
        if delete_overdue_data_time <= self.expired_time:
            self.db.cursor.execute(
                'delete from scresults_test where var_title not in (select p.path from sc_paths_online p)')
            self.db.connection.commit()
            self.expired_time = 0
            print('Overdue data was deleted!')


'''Поиск всех id каналов для всех Path из sc_paths with Online status'''


class Channel_id_and_type(object):
    def __init__(self, channel_id, channel_type):
        self.channel_id = channel_id
        self.channel_type = channel_type


def get_path_parent_tem(path):
    return path[::-1].split('/', 1)[-1][::-1]


def get_path_name_tem(path):
    return path.split('/')[-1]


class Tem_db_getter(Db_getter_setter):
    def __init__(self, time_interval, db_manager, path):
        super(Tem_db_getter, self).__init__(5, get_source_of_var(path), db_manager)
        self.variable_info = path
        self.path = get_path_of_var(path)
        self.is_it_new = 1
        self.param = get_param_of_var(path)

    def getting_older(self):
        self.is_it_new = 0

    def getter(self, res_dict):
        if self.interval_time <= self.expired_time:
            query_for_channel = "select * from v_dir where parent=%s and name = %s"
            args_for_channel = (get_path_parent_tem(self.path), get_path_name_tem(self.path))
            self.db.cursor.execute(query_for_channel, args_for_channel)
            data = self.db.cursor.fetchone()
            if self.param == 'value':
                res_dict[self.variable_info] = getter_for_lastinteger_lastdouble_value(self.db.cursor, data['id'],
                                                                                       data['type'])
                print("Got variable value. Path: " + self.path)
            if self.param == 'fvalue':
                self.db.cursor.execute(
                    "select id from v_dir where parent='/snd/scalers/increments' and name = 'timelive'")
                timelive_channel = self.db.cursor.fetchone()
                res_dict[self.variable_info] = getter_for_lastinteger_lastdouble_fvalue(self.db.cursor, data['id'],
                                                                                        data['type'], timelive_channel)
                print("Got variable value. Path: " + self.path)
            if self.param == 'stamp':
                res_dict[self.variable_info] = getter_for_lastinteger_lastdouble_stamp(self.db.cursor, data['id'],
                                                                                       data['type'])
                print("Got variable stamp. Path: " + self.path)
            if self.param == 'prev':
                res_dict[self.variable_info] = getter_for_integer_double_prev(self.db.cursor, data['id'],
                                                                              data['type'])
                print("Got variable prev. Path: " + self.path)
            if self.param == 'prev_stamp':
                res_dict[self.variable_info] = getter_for_integer_double_prev_stamp(self.db.cursor, data['id'],
                                                                                    data['type'])
                print("Got variable prev_stamp. Path: " + self.path)
            self.expired_time = 0
            if 'sum' in self.param or 'min' in self.param or 'max' in self.param or 'avg' in self.param:
                res_dict[self.variable_info] = getter_for_integer_double_sample(self.db.cursor, data['id'],
                                                                                data['type'], self.param)
                print("Got variable smth sample. Path: " + self.path)


# Функция для создания массива гетеров!


def create_getters_for_tem(django_db_connection, getter_dict, db_manager):
    django_db_connection.db.cursor.execute(
        'SELECT s1.path,s1.interval_time FROM sc_paths_online s1 LEFT JOIN sc_paths_online s2 ON s1.path = s2.path AND s2.interval_time < s1.interval_time WHERE s2.path_id IS NULL')
    data = django_db_connection.db.cursor.fetchall()
    for a in data:
        if a['path'] in getter_dict:
            print('Геттер с path: ' + str(a['path']) + ' уже существует, создавать новый не нужно')
            getter_dict[a['path']].is_it_new = 1
        else:
            getter_dict[a['path']] = (Tem_db_getter(a['interval_time'], db_manager, a['path']))
            print('Создан новый гетер. Его path: ' + str(
                a['path']) + '. Его интервал обновления: ' + str(
                a['interval_time']) + ' секунд.')


def execute_getters_for_tem(res_dict, getters):
    for key, value in getters.iteritems():
        value.getter(res_dict)


def update_getters_time_for_tem(getters, update_time):
    for key, value in getters.iteritems():
        value.update_time(update_time)
        value.getting_older()


def delete_old_getters(getters):
    new_getters = dict((key, value) for key, value in getters.iteritems() if value.is_it_new != 0)
    return new_getters


manager_of_db = Manager()  # создали менеджера db
setter_django = Setter_django(manager_of_db)  # Создали сеттер
results = dict()  # словарь для результатов (далее с его помощью будем заполнять БД django)
tem_getters = dict()  # словарь для всех гетеров с tem
time_checker = 0

while True:
    current_time = time.time()
    create_getters_for_tem(setter_django, tem_getters, manager_of_db)
    tem_getters = delete_old_getters(tem_getters)
    execute_getters_for_tem(results, tem_getters)
    setter_django.input_in_django_db(results)
    setter_django.delete_overdue_data()
    if (time.time() - current_time) < 5:
        time.sleep(5 - (time.time() - current_time))
    time_checker += time.time() - current_time
    print("Прошло времени: " + str(time_checker) + 'сек')
    lead_time = time.time() - current_time
    update_getters_time_for_tem(tem_getters, lead_time)
    setter_django.update_time(lead_time)

# close_cursor_connection(djangodb_cursor, djangodb_connection)
# clbdb.db_close_connection()
# admdb.db_close_connection()
print("everything is working!")
