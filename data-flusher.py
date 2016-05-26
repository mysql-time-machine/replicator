#!/usr/bin/python

import click
import MySQLdb
import MySQLdb.cursors
import Queue
import sys
import threading
from time import sleep
from operator import itemgetter

class Cursor(MySQLdb.cursors.CursorStoreResultMixIn, MySQLdb.cursors.CursorTupleRowsMixIn, MySQLdb.cursors.BaseCursor):
    pass


class CopyThread(threading.Thread):
    def __init__(self, queue, config):
        super(CopyThread, self).__init__()
        self.queue = queue
        self.config = config

    def run(self):
        while True:
            table = self.queue.get()
            if table is None:
                break
            if 'table' in self.config and table[1] not in self.config['table']:
                continue
            self.config['method'].do_copy(self.config, table)


class BlackholeCopyMethod(object):

    def pre(self, config, tables):
        source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
        cursor = source.cursor()
        done = []
        sql = 'reset master'
        print sql
        cursor.execute(sql)
        for table in tables:
            if (table[0], table[1]) in done:
                continue
            sql = 'show create table `{}`.`{}`;'.format(table[0], table[1])
            print sql
            cursor.execute(sql)
            create_table_sql =  cursor.fetchall()[0][1]
            sql = 'use {}'.format(table[0])
            print sql
            cursor.execute(sql)
            sql = 'set sql_log_bin=0'
            print sql
            cursor.execute(sql)
            sql = 'rename table `{}`.`{}` to `{}`.`_{}_old`;'.format(table[0], table[1], table[0], table[1])
            print sql
            cursor.execute(sql)
            cursor = source.cursor()
            sql = 'set sql_log_bin=1'
            print sql
            cursor.execute(sql)
            sql = 'create table `{}`.`{}` like `{}`.`_{}_old`;'.format(table[0], table[1], table[0], table[1])
            sql = create_table_sql
            print sql
            cursor.execute(sql)
            cursor = source.cursor()
            sql = 'set sql_log_bin=0'
            print sql
            cursor.execute(sql)
            sql = 'alter table `{}`.`{}` engine=blackhole;'.format(table[0], table[1])
            print sql
            cursor.execute(sql)
            sql = 'set sql_log_bin=1'
            print sql
            cursor.execute(sql)
            done.append((table[0], table[1]))

    def do_copy(self, config, table):
        source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
        cursor = source.cursor()
        if len(table) == 3:
            sql = """SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s and table_name = %s and column_key='PRI'"""
            print sql % (table[0], table[1])
            cursor.execute(sql, (table[0], table[1]))
            primary_key = cursor.fetchall()
            if len(primary_key) == 1 and primary_key[0][1] in ['tinyint', 'smallint', 'int', 'bigint']:
                self.chunked_copy(config, table, primary_key[0][0])
                return
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`_{}_old` PARTITION ({})'.format(table[0], table[1], table[0], table[1], table[2])
        else:
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`_{}_old`'.format(table[0], table[1], table[0], table[1])
        print sql
        cursor.execute(sql)
        source.commit()
        cursor.close()

    def chunked_copy(self, config, table, primary_key):
        source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor)
        cursor = source.cursor()
        sql = 'SELECT `{}` FROM `{}`.`_{}_old` PARTITION ({})'.format(primary_key, table[0], table[1], table[2])
        print sql
        cursor.execute(sql)
        ids = cursor.fetchall()
        cursor.close()
        ids = map(itemgetter(0), ids)
        cursor = source.cursor()
        offset = 0
        limit = 999
        while offset < len(ids):
            chunk = ids[offset:limit]
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`_{}_old` PARTITION ({}) WHERE id IN ({})'.format(table[0], table[1], table[0], table[1], table[2], ','.join([str(i) for i in chunk]))
            print 'Inserting chunk {} to {}'.format(min(chunk), max(chunk))
            cursor.execute(sql)
            source.commit()
            offset = limit
            limit += 1000
        cursor.close()

    def post(self, config, tables):
        source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
        cursor = source.cursor()
        sql = 'set sql_log_bin=0'
        print sql
        cursor.execute(sql)
        done = []
        for table in tables:
            if (table[0], table[1]) in done:
                continue
            sql = 'drop table `{}`.`{}`;'.format(table[0], table[1])
            print sql
            cursor.execute(sql)
            sql = 'rename table `{}`.`_{}_old` to `{}`.`{}`;'.format(table[0], table[1], table[0], table[1])
            print sql
            cursor.execute(sql)
            done.append((table[0], table[1]))
        sql = 'set sql_log_bin=1'
        print sql
        cursor.execute(sql)

def get_tables(config):
    source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
    cursor = source.cursor()
    cursor.execute('SELECT table_schema, table_name from information_schema.tables')
    tables = cursor.fetchall()
    cursor.close()
    cursor = source.cursor()
    cursor.execute('SELECT table_schema, table_name, partition_name from information_schema.partitions where partition_name is not null')
    partitions = cursor.fetchall()
    cursor.close()
    result = []
    for table in tables:
        if 'db' in config and table[0] not in config['db']:
            continue
        if table[0] in ['sys', 'mysql', 'information_schema', 'performance_schema', 'booking_meta']:
            continue
        partitioned = False
        for partition in partitions:
            if partition[1] == table[1]:
                result.append(partition)
                partitioned = True
        if not partitioned:
            result.append(table)
    return result

def queue_tables(config, tables):
    queue = Queue.Queue()

    threads = []
    for i in range(16):
        t = CopyThread(queue, config)
        t.daemon = True
        t.start()
        threads.append(t)

    for table in tables:
        queue.put(table)

    for i in range(16):
        queue.put(None)

    for t in threads:
        t.join()

@click.command()
@click.option('--mycnf', default='/root/.my.cnf', help='my.cnf with connection settings')
@click.option('--db', required=False, help='comma separated list of databases to copy. Leave blank for all databases')
@click.option('--table', required=False, help='comma separated list of tables to copy. Leave blank for all tables')
@click.option('--stop-slave/--no-stop-slave', default=True, help='stop the replication thread whilst running the copy')
@click.option('--start-slave/--no-start-slave', default=False, help='restart the replication thread after running the copy')
@click.option('--method', default='BlackholeCopy', help='Copy method class')
@click.option('--host', help='Host name')
def run(mycnf, db, table, stop_slave, start_slave, method, host):
    config = {
        'source' : mycnf,
    }
    if host:
        config['host'] = host
    if db:
        config['db'] = db.split(',')
    if table:
        config['table'] = table.split(',')
    if method == 'BlackholeCopy':
        config['method'] = BlackholeCopyMethod()
    source = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
    cursor = source.cursor()
    if stop_slave:
        sql = 'stop slave'
        print sql
        cursor.execute(sql)
    tables = get_tables(config)
    config['method'].pre(config, tables)
    queue_tables(config, tables)
    config['method'].post(config, tables)
    if start_slave:
        sql = 'start slave'
        print sql
        cursor.execute(sql)
    sys.exit(0)

if __name__ == '__main__':
    run()
