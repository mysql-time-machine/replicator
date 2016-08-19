## Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### Usage
***WARNING: you should NEVER run binlog-flusher on the MySQL master. Binlog flusher renames all tables during the blackhole copy and if the program does not finish successfully, the database can be left in a inconsistent state and in worst case you will need to reclone the database. The probability of this is low, but still, if it happens, you do NOT want this to happen on MySQL master.***

Assuming you adhere to the above warning, you can flush the contents of a database to the binlog with:
````
python data-flusher.py --mycnf .my.cnf --host $host [--db $db] [--table $table]
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