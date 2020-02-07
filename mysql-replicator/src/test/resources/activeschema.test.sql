-- This test makes use of only one container, so we cohost the active_schema alongside the regular db;

CREATE DATABASE active_schema;
CREATE DATABASE a_second_test;

use replicator;

CREATE TABLE test1 (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) CHARACTER SET utf8 NOT NULL,
     PRIMARY KEY (id)
);

CREATE TABLE a_second_test.test1_new LIKE replicator.test1;
CREATE TABLE a_second_test.test1 LIKE replicator.test1;

/* test ? */ RENAME TABLE `test1` TO `a_second_test`.`test1_old_rename`, `a_second_test`.test1_new TO `test1`;
/* test2 */ rename table a_second_test.test1 to test1_other_db;

BEGIN;
    INSERT INTO test1 (name) VALUES ('test');
COMMIT;

BEGIN;
    CREATE TABLE test1_new LIKE test1;
    ALTER TABLE test1_new ADD COLUMN ts TIMESTAMP DEFAULT '1970-01-01 01:00:00';
COMMIT;

BEGIN;
    RENAME TABLE test1 TO test1_old, test1_new TO test1;
COMMIT;

BEGIN;
    DROP TABLE test1_old;
COMMIT;

BEGIN;
    INSERT INTO test1 (name, ts) VALUES ('test', NOW() );
COMMIT;