package com.booking.replication.it.hbase.impl

import com.booking.replication.commons.services.ServicesControl
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTest
import com.booking.replication.it.hbase.ReplicatorHBasePipelineIntegrationTestRunner
import com.booking.replication.it.util.MySQL
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
 * Created by dbatheja on 20/12/2019.
 */
class ValidationTestImpl implements ReplicatorHBasePipelineIntegrationTest {
    static String SCHEMA_NAME = "replicator"
    static String VALIDATION_CONSUMER_GROUP = "validation-group"
    static String TABLE_NAME = "sometable"
    static int TOTAL_DMLS = 1000

    @Override
    String testName() {
        return "HbaseValidationTest"
    }

    @Override
    void doAction(ServicesControl mysqlReplicant) {
        // get handle
        def replicant = MySQL.getSqlHandle(
                false,
                SCHEMA_NAME,
                mysqlReplicant
        )

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE
            %s (
            pk_part_1         varchar(5) NOT NULL DEFAULT '',
            pk_part_2         int(11)    NOT NULL DEFAULT 0,
            randomInt         int(11)             DEFAULT NULL,
            randomVarchar     varchar(32)         DEFAULT NULL,
            PRIMARY KEY       (pk_part_1,pk_part_2),
            KEY randomVarchar (randomVarchar),
            KEY randomInt     (randomInt)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """, TABLE_NAME)

        replicant.execute(sqlCreate)
        replicant.commit()

        def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"

        for (int i=0; i<TOTAL_DMLS; i++){
            replicant.execute(sprintf(
                    "INSERT INTO %s %s VALUES ('user',%d,%d,'c%d');", TABLE_NAME, columns, i,i,i
            ))
            replicant.commit()
        }

        replicant.close()
    }

    @Override
    Object getExpectedState() {
        return (int) (TOTAL_DMLS/(Integer.parseInt(ReplicatorHBasePipelineIntegrationTestRunner.VALIDATION_THROTTLE_ONE_EVERY)))
    }

    @Override
    Object getActualState() throws IOException {

        Map<String, Object> kafkaConfiguration = new HashMap<>();
        kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ReplicatorHBasePipelineIntegrationTestRunner.VALIDATION_BROKER);
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, VALIDATION_CONSUMER_GROUP);
        kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // println(sprintf("---------[Getting actual state from Kafka Topic %s]---------", ReplicatorHBasePipelineIntegrationTestRunner.VALIDATION_TOPIC))
        int messagesToValidate = 0
        try {
            Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())
            consumer.subscribe(Collections.singleton(ReplicatorHBasePipelineIntegrationTestRunner.VALIDATION_TOPIC));

            final int stopTryingAfter = 10; int zeroRecordsFound = 0;
            while (true) {
                final ConsumerRecords<Long, String> consumerPollResult = consumer.poll(1000);
                if (consumerPollResult.count() == 0) {
                    zeroRecordsFound++;
                    if (zeroRecordsFound > stopTryingAfter) break;
                    else continue;
                }
                consumerPollResult.forEach({ record ->
                    messagesToValidate ++
                });
                consumer.commitAsync();
            }
            consumer.close();
            System.out.println("Total records found in " + ReplicatorHBasePipelineIntegrationTestRunner.VALIDATION_TOPIC + " = " + messagesToValidate.toString());
        } catch (Exception e){
            println(e)
        } finally {
            return messagesToValidate
        }
    }

    @Override
    boolean actualEqualsExpected(Object expected, Object actual) {
        int exp = (int) expected
        int act = (int) actual
        return exp == act
    }
}
