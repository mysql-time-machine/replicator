package com.booking.replication.applier.message.format.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import scala.collection.mutable.MutableList;

public class AvroUtils {

    static int MAGIC_BYTE = 0;


    public static byte[] serializeAvroGenericRecordWithSchemaIdPrepend(GenericRecord genericRecord, Integer schemaId) throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        outputStream.write(MAGIC_BYTE);
        outputStream.write(ByteBuffer.allocate(4).putInt(schemaId).array());

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(genericRecord.getSchema());

        datumWriter.write(genericRecord, encoder);

        encoder.flush();

        outputStream.close();

        byte[] byteArray =  outputStream.toByteArray();

        return byteArray;
    }

    public static GenericRecord deserializeAvroBlob(byte[] blob, Schema schema) throws IOException {

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(blob);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        MutableList<GenericRecord> records = new MutableList<GenericRecord>();

        while (!decoder.isEnd()) {
            GenericRecord item = datumReader.read(null, decoder);
            records.appendElem(item);
        }

        return records.head();
    }

    public static SerializedEvent extractSerializedEvent(byte[] messageBlob) {
        int schemaId =  ByteBuffer.wrap(Arrays.copyOfRange(messageBlob, 1, 5)).getInt();
        byte[] avroData = Arrays.copyOfRange(messageBlob, 5, messageBlob.length);
        SerializedEvent serializedEvent = new SerializedEvent(schemaId,avroData);
        return serializedEvent;
    }

}
