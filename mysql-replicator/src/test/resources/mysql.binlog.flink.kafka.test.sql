RESET MASTER;

use replicator;

CREATE TABLE animal (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name    VARCHAR(30) CHARACTER SET utf8 NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;
INSERT INTO animal (name) VALUES ('lion');
COMMIT;

BEGIN;
INSERT INTO animal (name) VALUES ('tiger');
COMMIT;

BEGIN;
INSERT INTO animal (name) VALUES ('panther');
COMMIT;

BEGIN;
INSERT INTO animal (name) VALUES ('cat');
COMMIT;

BEGIN;
INSERT INTO animal (name) VALUES ('bird');
COMMIT;