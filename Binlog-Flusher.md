## Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### Usage
***WARNING: you should NEVER run binlog-flusher on the MySQL master. Binlog flusher renames all tables during the blackhole copy and if the program does not finish successfully, the database can be left in an inconsistent state and in worst case you will need to reclone the database. The probability of this is low, but still, if it happens, you do NOT want this to happen on MySQL master.***

Assuming you adhere to the above warning, you can flush the contents of a database to the binlog with:

````
python data-flusher.py [--mycnf .my.cnf] [--db $db] [--table $table] [--stop-slave/--no-stop-slave] [--start-slave/--no-start-slave] --host $host [--skip $skip]
````

Where .my.cnf contains the admin privileges used for the blackhole_copy of initial snapshot.

````
[client]
user=admin
password=admin
````

Then start replication with

````
mysql> start slave;
````

In case something happened, the database is left in an inconsistent state, you can run db-recovery.py to recovery the database.

````
python db-recovery.py [--mycnf .my.cnf] [--db $db] [--table $table] [--stop-slave/--no-stop-slave] [--start-slave/--no-start-slave] --host $host --hashfile $hashfile [--skip $skip]
````

Where $hashfile contains the mappings from the backup table name to original table name.

````
_BKTB_1, $tablename1$
_BKTB_2, $tablename2$
....
````
