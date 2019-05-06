USE replicator;

BEGIN;

UPDATE organisms SET lifespan = 20.5 WHERE name = 'cat';
UPDATE organisms SET lifespan = 25.14 WHERE name = 'penguin';
UPDATE organisms SET lifespan = 250.18 WHERE name = 'whale';
UPDATE organisms SET lifespan = 80.90 WHERE name = 'ostrich';

COMMIT;

BEGIN;

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('elephant', 100, b'10111010', 'animalia');

COMMIT;

BEGIN;

DELETE FROM organisms WHERE id = 3;
DELETE FROM organisms WHERE id = 4;

COMMIT;