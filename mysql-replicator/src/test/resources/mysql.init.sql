USE replicator;

CREATE TABLE organisms (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) NOT NULL,
     lifespan INTEGER NOT NULL,
     kingdom ENUM('animalia', 'plantae') NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO organisms (name, lifespan, kingdom) VALUES ('dog', 20, 'animalia');
INSERT INTO organisms (name, lifespan, kingdom) VALUES ('cat', 15, 'animalia');
INSERT INTO organisms (name, lifespan, kingdom) VALUES ('penguin', 20, 'animalia');
INSERT INTO organisms (name, lifespan, kingdom) VALUES ('whale', 300, 'animalia');
INSERT INTO organisms (name, lifespan, kingdom) VALUES ('ostrich', 75, 'animalia');

COMMIT;

INSERT INTO organisms (name, lifespan, kingdom) VALUES ('horse', 25, 'animalia');

BEGIN;

UPDATE organisms SET name = 'lion' where id = 2;

COMMIT;

BEGIN;

DELETE FROM organisms WHERE id = 1;
DELETE FROM organisms WHERE id = 2;

COMMIT;

BEGIN;

INSERT INTO organisms (name, lifespan, kingdom) VALUES ('tree', 100, 'plantae');
INSERT INTO organisms (name, lifespan, kingdom) VALUES ('grass', 1, 'plantae');

COMMIT;