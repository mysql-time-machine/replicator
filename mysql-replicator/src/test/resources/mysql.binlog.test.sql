use replicator;

CREATE TABLE animal (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) CHARACTER SET utf8 NOT NULL,
     PRIMARY KEY (id)
);

CREATE TABLE _animal_new (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) CHARACTER SET utf8 NOT NULL,
     sc_name VARCHAR(30) CHARACTER SET utf8 DEFAULT NULL,
     PRIMARY KEY (id)
);

BEGIN;
INSERT INTO animal (name) VALUES ('tiger');
COMMIT;

BEGIN;
INSERT INTO _animal_new (name) VALUES ('tiger');
COMMIT;


BEGIN;
RENAME TABLE animal TO _animal_old, _animal_new TO animal;
COMMIT;

BEGIN;
DROP TABLE _animal_old;
COMMIT;

BEGIN;
INSERT INTO animal (name, sc_name) VALUES ('tiger', 'Panthera tigris');
COMMIT;