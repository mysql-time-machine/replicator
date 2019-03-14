#!/bin/bash
if [ "$1" = "debug" ]
then
    DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,address=4321,suspend=y"
    CHAIN=$2
else
    CHAIN=$1
fi

java -Xms1024m -Xmx2048m -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Dlog4j.configurationFile=conf/log4j2.${CHAIN}.xml ${DEBUG} -jar mysql-replicator-0.16.0-rc1-SNAPSHOT/mysql-replicator-0.16.0-rc1-SNAPSHOT.jar --config-file conf/replicator.${CHAIN}.yml &

# remote profiling
#java -Xms1024m -Xmx4028m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.s   un.management.jmxremote.ssl=false -Djava.rmi.server.hostname=0.0.0.0 -Djava.u   til.logging.manager=org.apache.logging.log4j.jul.LogManager -Dlog4j.configurat   ionFile=conf/log4j2.${CHAIN}.xml ${DEBUG} -jar mysql-replicator-0.16.0-rc1-SNAPSHOT/mysql-replicator-0.16.0-rc1-SNAPSHOT.jar --config-file conf/replicat   o   r.${CHAIN}.yml &
