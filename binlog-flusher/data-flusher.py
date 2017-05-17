#!/usr/bin/env python

import click
from datetime import datetime
import logging
import MySQLdb
import MySQLdb.cursors
import sys
import Queue
import threading
from threading import Thread
import signal
from time import sleep
from operator import itemgetter

class Cursor(MySQLdb.cursors.CursorStoreResultMixIn, MySQLdb.cursors.CursorTupleRowsMixIn, MySQLdb.cursors.BaseCursor):
    pass

class StateHolder():
    def __init__(self):
        pass
    def set_is_running(self, is_running_var):
        self.is_running_var = is_running_var
    def is_running(self):
        return self.is_running_var
    def set_errors(self, errors):
        self.errors = errors
    def has_errors(self):
        return self.errors


class CopyProcess(Thread):
    def __init__(self, queue, config, main):
        super(CopyProcess, self).__init__()
        self.queue = queue
        self.config = config
        self.main = main

    def run(self):
        while self.main.state_holder.is_running() and not self.queue.empty():
            try:
                table = self.queue.get(False)
                if 'table' in self.config and table[1] not in self.config['table']:
                    continue
                self.main.do_copy(self.config, table)
                if self.main.state_holder.is_running():
                    self.queue.task_done()
            except Queue.Empty as e:
                pass


class ConnectionDispatch(Thread):
    def __init__(self, config):
        super(ConnectionDispatch, self).__init__()
        self.connection_ids = []
        self.die = False
        self.daemon = True
        self.config = config
        self.MAX_RETRIES = 10

    def get_source(self):
        retry = 1
        while True:
            logger.info("trying to connect to mysql...")
            try:
                logger.info("attempt %d" % retry)
                src = MySQLdb.connect(read_default_file=self.config['source'], cursorclass=Cursor, host=self.config['host'])
                self._add_id(src.thread_id())
                return src
            except Exception as e:
                logger.info(str(e))
                if (retry > self.MAX_RETRIES):
                    raise
                sleep(1)
                retry += 1

    def _add_id(self, tid):
        logger.info("thread id {} added..".format(tid))
        self.connection_ids += [tid]

    def _reset_connection(self):
        self.connection_ids = []

    def terminate(self):
        self.die = True

    def run(self):
        while not self.die:
            pass
        # We create another MySQL connection to kill all the process
        logger.info("Terminating all connections")
        hc_killer = MySQLdb.connect(read_default_file=self.config['source'], cursorclass=Cursor, host=self.config['host'])
        hc_cursor = hc_killer.cursor()
        for tid in self.connection_ids:
            logger.info("Killing thread id: {}".format(tid))
            try:
                hc_cursor.execute('kill {}'.format(tid))
            except MySQLdb.OperationalError:
                logger.warn("Thread {} doesn't exist".format(tid))
        self._reset_connection()
        hc_killer.close()

class BlackholeCopyMethod(object):
    def __init__(self, config, tables, condis):
        self.hashCount = 0
        self.hashRep = {}
        self.MAX_RETRIES = 10
        self.config = config
        self.tables = tables
        self.threads = []
        self.queue = Queue.Queue()
        self.conDis = condis
        self.state_holder = StateHolder()
        self.state_holder.set_is_running(True)
        self.state_holder.set_errors(False)
        signal.signal(signal.SIGINT, self.shutdown_hook)
        signal.signal(signal.SIGTERM, self.shutdown_hook)

    def shutdown_hook(self, signum, frame):
        logger.warn("Running shutdown_hook")
        self.state_holder.set_is_running(False)
        self.state_holder.set_errors(True)

    def retcode(self):
        if self.state_holder.has_errors():
            return 1
        return 0

    def queue_tables(self, config, tables):
        for table in tables:
            self.queue.put(table)

    def consumer_queue(self):
        for i in range(min(self.queue.qsize(), 16)):
            p = CopyProcess(self.queue, self.config, self)
            p.daemon = True
            p.start()
            self.threads.append(p)

    def join(self):
        while threading.active_count() > 2:
            try:
                while threading.active_count() > 2:
                    sleep(0.1)
                    # Fake join
            except Exception as e:
                logger.warn("There is an exception, probably you stop the process: " + str(e))
                self.conDis.terminate()

    def has_key(self, table_name):
        return table_name in self.hashRep

    def remove_key(self, table_name):
        self.hashRep.pop(table_name)

    def get_hash(self, table_name):
        if not self.has_key(table_name):
            if table_name[0:6] == '_BKTB_':
                self.post()
                logger.error("Broken database")
                raise Exception("Did you forget to recover the database? Try to run db-recovery.py before!")
            self.hashCount += 1
            with open (hashmapFileName, 'a') as f:
                f.write("_BKTB_%d,%s\n" % (self.hashCount, table_name))
            logger.info("Mapper %s to _BKTB_%d" % (table_name, self.hashCount))
            return '_BKTB_%d' % self.hashCount
        return self.hashRep[table_name]

    def set_hash(self, table_name, hash_table_name):
        self.hashRep[table_name] = hash_table_name

    def execute_sql(self, cursor, sql):
        logger.info(sql)
        return cursor.execute(sql)

    def pre(self, config, tables):
        source = self.conDis.get_source()
        cursor = source.cursor()
        done = []
        self.execute_sql(cursor, 'reset master')
        for table in tables:
            if (table[0], table[1]) in done:
                continue
            self.execute_sql(cursor, 'show create table `{}`.`{}`;'.format(table[0], table[1]))
            create_table_sql =  cursor.fetchall()[0][1]
            self.execute_sql(cursor, 'use {}'.format(table[0]))
            self.execute_sql(cursor, 'set sql_log_bin=0')

            hash_table_name = self.get_hash(table[1])
            self.execute_sql(cursor, 'rename table `{}`.`{}` to `{}`.`{}`;'.format(table[0], table[1], table[0], hash_table_name))
            self.set_hash(table[1], hash_table_name)

            cursor = source.cursor()
            self.execute_sql(cursor, 'set sql_log_bin=1')
            self.execute_sql(cursor, create_table_sql)

            cursor = source.cursor()
            self.execute_sql(cursor, 'set sql_log_bin=0')

            self.execute_sql(cursor, 'show index from `{}`.`{}` where `Key_name` != "PRIMARY";'.format(table[0], table[1]))
            indexes = set(reduce(lambda arr, x: arr + [x[2]], cursor.fetchall(), []))
            for index in indexes:
                sql = 'alter table `{}`.`{}` drop index `{}`;'.format(table[0], table[1], index)
                try:
                    self.execute_sql(cursor, sql)
                except MySQLdb.OperationalError as e:
                    logger.warn("Failed to run query: {}. Error: {}".format(sql, e))

            self.execute_sql(cursor, 'alter table `{}`.`{}` engine=blackhole;'.format(table[0], table[1]))
            self.execute_sql(cursor, 'set sql_log_bin=1')

            done.append((table[0], table[1]))

    def do_copy(self, config, table):
        primaries_to_chunk = ['tinyint', 'smallint', 'mediumint', 'int', 'bigint']
        table_size_to_chunk = 512 * 1024 * 1024

        source = self.conDis.get_source()
        cursor = source.cursor()

        # is chunked copy possible
        sql = """SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s and table_name = %s and column_key='PRI'"""
        logger.info(sql % (table[0], table[1]))
        cursor.execute(sql, (table[0], table[1]))
        primary_key = cursor.fetchall()

        # is table big
        # big tables should be copied by chunks to prevent having huge binary logs. Despite that we shouldn't chunk small tables because of slowdown
        sql = """SELECT data_length FROM information_schema.tables WHERE table_schema = %s and table_name = %s"""
        logger.info(sql % (table[0], table[1]))
        cursor.execute(sql, (table[0], self.get_hash(table[1])))
        table_size = cursor.fetchall()
        logger.info("Size of %s.%s(%s) = %d" % (table[0], self.get_hash(table[1]), table[1], table_size[0][0]) )

        # is table partitioned
        if len(table) == 3:
            if table_size[0][0] > table_size_to_chunk and len(primary_key) >= 1 and primary_key[0][1] in primaries_to_chunk:
                self.chunked_partition_copy(config, table, primary_key[0][0])

                return
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}` PARTITION ({})'.format(table[0], table[1], table[0], self.get_hash(table[1]), table[2])
        else:
            if table_size[0][0] > table_size_to_chunk and len(primary_key) >= 1 and primary_key[0][1] in primaries_to_chunk:
                self.chunked_copy(config, table, primary_key[0][0])
                return
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}`'.format(table[0], table[1], table[0], self.get_hash(table[1]))

        logger.info(sql)
        executed = False
        retryTimes = 0
        while self.state_holder.is_running() and not executed:
            try:
                cursor.execute(sql)
                source.commit()
                executed = True
            except Exception, MySQLdb.OperationalError:
                retryTimes += 1
                if retryTimes == self.MAX_RETRIES:
                    break
                source = self.conDis.get_source()
                cursor = source.cursor()
        if not executed:
            self.state_holder.set_errors(True)
            logger.error("We are unable to insert into %s.%s after %d retries" % (table[0], table[1], self.MAX_RETRIES))
        else:
            logger.info("Copy of %s.%s finished" % (table[0], table[1]))
        cursor.close()

    def chunked_copy(self, config, table, primary_key):
        source = self.conDis.get_source()
        cursor = source.cursor()
        self.execute_sql(cursor, 'SELECT MIN(`{}`), MAX(`{}`) FROM `{}`.`{}`'.format(primary_key, primary_key, table[0], self.get_hash(table[1])))
        ids = cursor.fetchall()
        cursor.close()
        pos = ids[0][0]
        ids_expected = ids[0][1] - ids[0][0]
        max_iterations = 100000
        limit = 10000
        done_pct = -1
        have_errors = True
        if ids_expected/max_iterations > limit:
            limit = ids_expected/max_iterations
        logger.info('Copying table {}.{} chunked on {} column, min={}, max={}, limit={}'.format(table[0], table[1], primary_key, ids[0][0], ids[0][1], limit))
        while self.state_holder.is_running() and pos < ids[0][1]:
            if pos + limit > ids[0][1]:
                newpos = ids[0][1]
            else:
                newpos = pos + limit
            new_done_pct = int(100 * (pos - ids[0][0]) / ids_expected)
            if new_done_pct > done_pct:
                done_pct = new_done_pct
                logger.info('Inserting table {}.{} chunk {} to {}. {}% done'.format(table[0], table[1], pos, newpos, done_pct))
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}` WHERE {} BETWEEN ({}) AND ({})'.format(table[0], table[1], table[0], self.get_hash(table[1]), primary_key, pos, newpos)
            executed = False
            retryTimes = 0
            while not executed:
                try:
                    cursor.execute(sql)
                    source.commit()
                    pos = newpos
                    executed = True
                except Exception, MySQLdb.OperationalError:
                    retryTimes += 1
                    if retryTimes == self.MAX_RETRIES:
                        break
                    source = self.conDis.get_source()
                    cursor = source.cursor()
            if not executed:
                self.state_holder.set_errors(True)
                have_errors = False
                break
        if have_errors:
            logger.info("Copy of %s.%s finished" % (table[0], table[1]))
        else:
            logger.error("We are unable to insert into Table: %s.%s after %d retries" % (table[0], table[1], self.MAX_RETRIES))
        cursor.close()

    def chunked_partition_copy(self, config, table, primary_key):
        source = self.conDis.get_source()
        cursor = source.cursor()
        self.execute_sql(cursor, 'SELECT MIN(`{}`), MAX(`{}`) FROM `{}`.`{}` PARTITION ({})'.format(primary_key, primary_key, table[0], self.get_hash(table[1]), table[2]))
        ids = cursor.fetchall()
        cursor.close()
        cursor = source.cursor()
        pos = ids[0][0]
        ids_expected = ids[0][1] - ids[0][0]
        max_iterations = 100000
        limit = 10000
        done_pct = -1
        have_errors = True
        if ids_expected/max_iterations > limit:
            limit = ids_expected/max_iterations
        logger.info('Copying table {}.{}.{} chunked on {} column, min={}, max={}, limit={}'.format(table[0], table[1], table[2], primary_key, ids[0][0], ids[0][1], limit))
        while self.state_holder.is_running() and pos < ids[0][1]:
            if pos + limit > ids[0][1]:
                newpos = ids[0][1]
            else:
                newpos = pos + limit
            new_done_pct = int(100 * (pos - ids[0][0]) / ids_expected)
            if new_done_pct > done_pct:
                done_pct = new_done_pct
                logger.info('Inserting table {}.{}.{} chunk {} to {}. {}% done'.format(table[0], table[1], table[2], pos, newpos, done_pct))
            sql = 'INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}` PARTITION ({}) WHERE {} BETWEEN ({}) AND ({})'.format(table[0], table[1], table[0], self.get_hash(table[1]), table[2], primary_key, pos, newpos)
            executed = False
            retryTimes = 0
            while not executed:
                try:
                    cursor.execute(sql)
                    source.commit()
                    pos = newpos
                    executed = True
                except Exception, MySQLdb.OperationalError:
                    retryTimes += 1
                    if retryTimes == self.MAX_RETRIES:
                        break
                    source = self.conDis.get_source()
                    cursor = source.cursor()
            if not executed:
                self.state_holder.set_errors(True)
                have_errors = False
                break
        if have_errors:
            logger.info("Copy of %s.%s.%s finished" % (table[0], table[1], table[2]))
        else:
            logger.error("We are unable to insert into Table: %s.%s.%s after %d retries" % (table[0], table[1], table[2], self.MAX_RETRIES))
        cursor.close()

    def post(self):
        print "Please wait while the DB is cleaning up..."
        source = self.conDis.get_source()
        cursor = source.cursor()
        self.execute_sql(cursor, 'set sql_log_bin=0')
        for table in self.tables:
            if self.has_key(table[1]):
                sql = "SELECT ENGINE from information_schema.tables where TABLE_SCHEMA='{}' and TABLE_NAME='{}';".format(table[0], table[1])
                cursor.execute(sql)
                engine = cursor.fetchall()
                if engine and engine[0][0] == "BLACKHOLE":
                    sql = "drop table if exists `{}`.`{}`".format(table[0], table[1])
                    logger.info(sql)
                    cursor.execute(sql)
                    sql = 'rename table `{}`.`{}` to `{}`.`{}`;'.format(table[0], self.get_hash(table[1]), table[0], table[1])
                    logger.info(sql)
                    cursor.execute(sql)
                else:
                    self.state_holder.set_errors(True)
                    logger.error("Can't recover table {}.{}({}). Engine not blackhole or unexists: {}".format(table[0], table[1], self.get_hash(table[1]), engine))
                self.remove_key(table[1])
        self.execute_sql(cursor, 'set sql_log_bin=1')

        self.conDis.terminate()


class DataFlusher(object):
    def get_tables(self, conDis, config):
        source = conDis.get_source()
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
            if table[0] in config['skip']:
                continue
            partitioned = False
            for partition in partitions:
                if partition[1] == table[1]:
                    result.append(partition)
                    partitioned = True
            if not partitioned:
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
@click.option('--skip', required=False, help='comma separated list of skip schemas')
def run(mycnf, db, table, stop_slave, start_slave, method, host, skip):
    flusher = DataFlusher()
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
    if skip:
        config['skip'].append(skip.split(','))
    conDis = ConnectionDispatch(config)
    conDis.start()
    tables = flusher.get_tables(conDis, config)
    source = conDis.get_source()
    cursor = source.cursor()
    if stop_slave:
        sql = 'stop slave'
        logger.info(sql)
        cursor.execute(sql)
    if method == 'BlackholeCopy':
        # Isn't it the only method?
        main = BlackholeCopyMethod(config, tables, conDis)
    main.pre(config, tables)
    main.queue_tables(config, tables)
    main.consumer_queue()
    main.join()
    main.post()
    if start_slave:
        sql = 'start slave'
        logger.info(sql)
        cursor.execute(sql)
    sys.exit(main.retcode())

if __name__ == '__main__':
    logger = logging.getLogger('dataFlusher')
    curTime = datetime.now()
    hdlr = logging.FileHandler('dataFlusher-%s.log' % curTime)
    hashmapFileName = 'flusherHash-%s.txt' % curTime
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    run()
