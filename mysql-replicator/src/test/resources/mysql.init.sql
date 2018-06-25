USE replicator;

CREATE TABLE animals (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) NOT NULL,
     lifespan INTEGER NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO animals (name, lifespan) VALUES ('dog', 20);
INSERT INTO animals (name, lifespan) VALUES ('cat', 15);
INSERT INTO animals (name, lifespan) VALUES ('penguin', 20);
INSERT INTO animals (name, lifespan) VALUES ('whale', 300);
INSERT INTO animals (name, lifespan) VALUES ('ostrich', 75);

COMMIT;

INSERT INTO animals (name, lifespan) VALUES ('horse', 25);

BEGIN;

UPDATE animals SET name = 'lion' where id = 2;

COMMIT;

BEGIN;

DELETE FROM animals WHERE id = 1;
DELETE FROM animals WHERE id = 2;

COMMIT;

CREATE TABLE plants (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) NOT NULL,
     lifespan INTEGER NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO plants (name, lifespan) VALUES ('tree', 100);
INSERT INTO plants (name, lifespan) VALUES ('grass', 1);

COMMIT;