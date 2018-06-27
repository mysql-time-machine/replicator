package com.booking.replication.util;

import com.booking.replication.applier.kafka.RowListMessage;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.exceptions.RowListMessageDeserializationException;
import com.booking.replication.exceptions.RowListMessageSerializationException;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.table.TableSchemaVersion;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.serializer.SerializerException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Json Builder.
 */

public class JsonBuilder {

    private static final ObjectMapper om = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonBuilder.class);

    public static String augmentedRowToJson(AugmentedRow augmentedRow) {
        String json = null;
        try {
            json = om.writeValueAsString(augmentedRow);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String rowListMessageToJSON(RowListMessage rowListMessage)
            throws RowListMessageSerializationException {
        String json = null;
        try {
            json = om.writeValueAsString(rowListMessage);
        } catch (IOException e) {
            throw new RowListMessageSerializationException("ERROR: could not serialize RowListMessage object.");
        }
        return json;
    }

    public static RowListMessage rowListMessageFromJSON(String jsonString)
            throws RowListMessageDeserializationException {
        RowListMessage rowListMessageFrom = null;
        try {
            rowListMessageFrom = om.readValue(jsonString, RowListMessage.class);
        } catch (IOException e) {
            throw new RowListMessageDeserializationException("ERROR: could not deserialize RowListMessage object from jsonString" + jsonString);
        }
        return rowListMessageFrom;
    }

    public static String schemaVersionToJson(ActiveSchemaVersion activeSchemaVersion) {
        String json = null;
        try {
            //json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(activeSchemaVersion);
            json = om.writeValueAsString(activeSchemaVersion);

        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaChangeEventToJson(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent) {
        String json = null;
        try {
            json = om.writeValueAsString(augmentedSchemaChangeEvent);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaTransitionSequenceToJson(HashMap<String,String> schemaTransitionSequence) {
        String json = null;
        try {
            json = om.writeValueAsString(schemaTransitionSequence);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaVersionTablesToJson(Map<String, TableSchemaVersion> schemaVersionTables) {
        String json = null;
        try {
            json = om.writeValueAsString(schemaVersionTables);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaCreateStatementsToJson(HashMap<String, String> schemaCreateStatements) {
        String json = null;
        try {
            json = om.writeValueAsString(schemaCreateStatements);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }
}
