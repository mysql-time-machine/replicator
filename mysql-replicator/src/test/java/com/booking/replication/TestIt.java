package com.booking.replication;


import com.booking.replication.applier.schema.registry.BCachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Future;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS;

@Ignore
public class TestIt {
    private static final String DDL_SCHEMA = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ddl\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"query\",\n" +
            "      \"type\": \"string\"\n" +
            "    }]\n" +
            "}";

    private static final String DDL_SCHEMA2 = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ddl\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"query\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "{\"name\":\"kingdom\",\"type\":[\"null\",\"string\"],\"default\":null}]" +
            "}";

    public static final String SCHEMA_REGISTRY_KVM = "http://akonale-hadoop-01.fab4.prod.booking.com:8888";
    public static final String SCHEMA_REGISTRY_LOCAL = "http://localhost:8081";

    @Test
    public void testLeadership() throws InterruptedException, IOException, RestClientException {
//        Schema schema2 = new Schema.Parser().parse(DDL_SCHEMA2);
//        Schema schema = new Schema.Parser().parse(DDL_SCHEMA);
        Schema schema1 = new Schema.Parser().parse(DDL_SCHEMA);

        IdentityHashMap<Schema, String> mymap = new IdentityHashMap<Schema, String>();
        HashMap<Schema, String> myhashmap = new HashMap<Schema, String>();
        mymap.put(schema1, "one");
        myhashmap.put(schema1, "one");

//        System.out.println(mymap.containsKey(schema));
//        System.out.println(myhashmap.containsKey(schema));
//
        BCachedSchemaRegistryClient client = new BCachedSchemaRegistryClient(SCHEMA_REGISTRY_KVM, 1000);
        client.updateCompatibility("test-test2-value", "backward");
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(client);
        for (int i = 0; i < 25; i++) {
            Schema schema = new Schema.Parser().parse(DDL_SCHEMA);
            kafkaAvroSerializer.register("test-test2-value", schema);
        }
        Schema schema2 = new Schema.Parser().parse(DDL_SCHEMA2);

        kafkaAvroSerializer.register("test-test2-value", schema2);


    }

    private byte[] serializeAvroMessage(GenericRecord rec, Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(schema.toString().getBytes());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);

        writer.write(rec, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    @Test
    public void testAvro() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(DDL_SCHEMA);

        final GenericRecord rec = new GenericData.Record(schema);
        rec.put("query", "myquery");

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_KVM, 1000);

        client.register("akonale-schema-value", rec.getSchema());
        HashMap<String, Object> properties = new HashMap<String, Object>();
//        properties.put("auto.register.schemas", false);
//        properties.put("schema.registry.url", SCHEMA_REGISTRY_);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(client);

        byte[] bytes = kafkaAvroSerializer.serialize("akonale-schema", rec);

        KafkaProducer<byte[], byte[]> producer = createProducer();
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("test", "key".getBytes(), bytes);
        Future<RecordMetadata> send = producer.send(record);
        RecordMetadata recordMetadata = send.get();
        System.out.println(recordMetadata.offset());

    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", ByteArraySerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_LOCAL);
        props.put(AUTO_REGISTER_SCHEMAS, false);
        return new KafkaProducer<>(props);
    }

    @Test
    public void testConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "a2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_LOCAL);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_LOCAL, 1000);

        while (true) {

            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(client);
                GenericRecord deserialize = (GenericRecord) kafkaAvroDeserializer.deserialize("", record.value());
//                System.out.println(key);
                System.out.println(deserialize.getSchema().toString());
                System.out.println(deserialize.get("query"));
            }
        }
    }


}
