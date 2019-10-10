
set @@global.binlog_row_metadata='FULL';

GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%' IDENTIFIED BY 'replicator';
FLUSH PRIVILEGES;
DO SLEEP(10);
SHOW MASTER STATUS;