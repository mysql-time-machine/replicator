USE replicator;

CREATE TABLE animals (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO animals (name) VALUES ('dog');
INSERT INTO animals (name) VALUES ('cat');
INSERT INTO animals (name) VALUES ('penguin');
INSERT INTO animals (name) VALUES ('lax');
INSERT INTO animals (name) VALUES ('whale');
INSERT INTO animals (name) VALUES ('ostrich');

COMMIT;

INSERT INTO animals (name) VALUES ('horse');

BEGIN;

UPDATE animals SET name = 'lion' where id = 1;
UPDATE animals SET name = 'tiger' where id = 2;

COMMIT;

BEGIN;

DELETE FROM animals WHERE id = 1;
DELETE FROM animals WHERE id = 2;

COMMIT;