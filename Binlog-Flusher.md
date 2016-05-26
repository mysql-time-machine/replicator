## Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### Usage
Flush database to binlog with:
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