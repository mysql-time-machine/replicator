USE replicator;

CREATE TABLE organisms (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) NOT NULL,
     lifespan TINYINT UNSIGNED NOT NULL,
     bits bit(8) NOT NULL,
     kingdom ENUM('animalia', 'plantae') NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('dog', 255, b'10101010', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('cat', 15, b'10101011', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('penguin', 20, b'00101010', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('whale', 300, b'10101110', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('ostrich', 75, b'10101111', 'animalia');

COMMIT;

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('horse', 25, b'10111010', 'animalia');

BEGIN;

UPDATE organisms SET name = 'lion' where id = 2;

COMMIT;

BEGIN;

DELETE FROM organisms WHERE id = 1;
DELETE FROM organisms WHERE id = 2;

COMMIT;

BEGIN;

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('tree', 100, b'10101010','plantae');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('grass', 1, b'10101010', 'plantae');

COMMIT;