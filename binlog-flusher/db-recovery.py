#!/usr/bin/env python

import click
from datetime import datetime
import logging
import MySQLdb
import MySQLdb.cursors
import sys
import signal
from time import sleep
from operator import itemgetter


class Cursor(MySQLdb.cursors.CursorStoreResultMixIn, MySQLdb.cursors.CursorTupleRowsMixIn, MySQLdb.cursors.BaseCursor):
    pass


def get_source(config):
    src = MySQLdb.connect(read_default_file=config['source'], cursorclass=Cursor, host=config['host'])
    return src

class RecoveryMethod(object):
    def __init__(self, config, tables):
        self.hashCount = 0
        self.hashRep = {}
        self.MAX_RETRIES = 3
        self.config = config
        self.tables = tables

    def build_hash(self):
        with open(self.config['hashfile'], 'r') as f:
            content = f.readlines()
            for line in content:
                key, value = line.strip().split(',')
                self.hashRep[key] = value

    def get_hash(self, table_name):
        if table_name not in self.hashRep:
            logger.error('Missing map of %s' % table_name)
            return ""
        return self.hashRep[table_name]

    def process(self):
        source = get_source(self.config)
        cursor = source.cursor()
        sql = 'set sql_log_bin=0'
        logger.info(sql)
        cursor.execute(sql)
        self.build_hash()
        for table in self.tables:
            if table[1] in self.hashRep:
                sql = 'rename table `{}`.`{}` to `{}`.`{}`;'.format(table[0], table[1], table[0], self.get_hash(table[1]))
                logger.info(sql)
                try:
                    cursor.execute(sql)
                except MySQLdb.OperationalError as e:
                    logger.error("Operation error for {} to {}. Error: {}".format(table[1], self.get_hash(table[1]), e))
                    sql = "SELECT ENGINE from information_schema.tables where TABLE_SCHEMA='{}' and TABLE_NAME='{}';".format(table[0], self.get_hash(table[1]))
                    cursor.execute(sql)
                    engine = cursor.fetchall()
                    if engine and engine[0][0] == "BLACKHOLE":
                        sql = "drop table if exists `{}`.`{}`".format(table[0], self.get_hash(table[1]))
                        logger.info(sql)
                        cursor.execute(sql)
                        sql = 'rename table `{}`.`{}` to `{}`.`{}`;'.format(table[0], table[1], table[0], self.get_hash(table[1]))
                        logger.info(sql)
                        cursor.execute(sql)
                    else:
                        logger.error("Can't recover table {}.{}. Engine not blackhole or unexists: {}".format(table[0], self.get_hash(table[1]), engine))
        sql = 'set sql_log_bin=1'
        logger.info(sql)
        cursor.execute(sql)


def get_tables(config):
    source = get_source(config)
    cursor = source.cursor()
    cursor.execute('SELECT table_schema, table_name from information_schema.tables')
    tables = cursor.fetchall()
    cursor.close()
    result = []
    for table in tables:
        if 'db' in config and table[0] not in config['db']:
            continue
        if table[0] in config['skip']:
            continue
        result.append(table)
    return result

@click.command()
@click.option('--mycnf', default='~/my.cnf', help='my.cnf with connection settings')
@click.option('--db', required=False, help='comma separated list of databases to copy. Leave blank for all databases')
@click.option('--table', required=False, help='comma separated list of tables to copy. Leave blank for all tables')
@click.option('--stop-slave/--no-stop-slave', default=True, help='stop the replication thread whilst running the copy')
@click.option('--start-slave/--no-start-slave', default=False, help='restart the replication thread after running the copy')
@click.option('--method', default='BlackholeCopy', help='Copy method class')
@click.option('--host', help='Host name')
@click.option('--hashfile', help='Hashmap file name')
@click.option('--skip', required=False, help='comma separated list of skip schemas')
def run(mycnf, db, table, stop_slave, start_slave, method, host, hashfile, skip):
    config = {
        'source' : mycnf,
        'skip': ['sys', 'mysql', 'information_schema', 'performance_schema']
    }
    if host:
        config['host'] = host
    if db:
        config['db'] = db.split(',')
    if table:
        config['table'] = table.split(',')
    if hashfile:
        config['hashfile'] = hashfile
    if skip:
        config['skip'].append(skip.split(','))
    tables = get_tables(config)
    config['method'] = RecoveryMethod(config, tables)
    source = get_source(config)
    cursor = source.cursor()
    if stop_slave:
        sql = 'stop slave'
        logger.info(sql)
        cursor.execute(sql)
    config['method'].process()
    if start_slave:
        sql = 'start slave'
        logger.info(sql)
        cursor.execute(sql)
    sys.exit(0)


if __name__ == '__main__':
    logger = logging.getLogger('DBRecovery')
    curTime = datetime.now()
    hdlr = logging.FileHandler('DBRecovery-%s.log' % curTime)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    run()
