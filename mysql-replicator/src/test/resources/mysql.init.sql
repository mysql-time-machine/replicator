USE replicator;

CREATE TABLE organisms (
     id INTEGER NOT NULL AUTO_INCREMENT,
     name VARCHAR(30) CHARACTER SET utf8 NOT NULL,
     lifespan TINYINT UNSIGNED,
     lifespan_small SMALLINT UNSIGNED,
     lifespan_medium MEDIUMINT UNSIGNED,
     lifespan_int INT UNSIGNED,
     lifespan_bigint  BIGINT UNSIGNED,
     bits bit(8) NOT NULL,
     soylent_dummy_id binary(16) DEFAULT NULL,
     dob timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     mydecimal decimal(27,9) NOT NULL DEFAULT '0.000000000',
     kingdom ENUM('animalia', 'plantae') NOT NULL,
     PRIMARY KEY (id)
);

BEGIN;

INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('example é',  255, 65535, 16777215, 4294967295, 18446744073709551615 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D8900'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('Ñandú', 240, 65500, 16770215, 4294897295, 18446744071615       , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('dog2', 255, 65535, 16777215, 4294967295, 18446744073709551111 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('sÃƒÂ¥', 255, 11135, 16222215, 4288888295, 18446744073709221615 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('யாமறிந்த', 255, 65535, 16333315, 4294967295, 18446744073709551615 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('dog5', 125, 22235, 16777215, 4294967295, 18446744072000551615 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');
INSERT INTO organisms (name, lifespan, lifespan_small, lifespan_medium, lifespan_int, lifespan_bigint, bits, soylent_dummy_id, mydecimal, kingdom) VALUES ('tägelîch', 255, 62225, 13333315, 4291111295, 18446744072222221615 , b'10101010', UNHEX('000001348BB470A5129E6C8D332D89CC'), 100,'animalia');

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('cat', 15, b'10101011', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('penguin', 20, b'00101010', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('whale', 240, b'10101110', 'animalia');
INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('ostrich', 75, b'10101111', 'animalia');

COMMIT;

BEGIN;

INSERT INTO organisms (name, lifespan, bits, kingdom) VALUES ('horse', 25, b'10111010', 'animalia');

COMMIT;

BEGIN;

UPDATE organisms SET name = 'lion' where id = 2;

COMMIT;

BEGIN;

DELETE FROM organisms WHERE id = 1;
DELETE FROM organisms WHERE id = 2;

COMMIT;

BEGIN;

ALTER table organisms ADD COLUMN lifespan_float FLOAT(7,4);

COMMIT;

BEGIN;

INSERT INTO organisms (name, lifespan, bits, kingdom, lifespan_float) VALUES ('tree', 100, b'10101010','plantae', 1.23);
INSERT INTO organisms (name, lifespan, bits, kingdom, lifespan_float) VALUES ('grass', 1, b'10101010', 'plantae', 2.45);

COMMIT;