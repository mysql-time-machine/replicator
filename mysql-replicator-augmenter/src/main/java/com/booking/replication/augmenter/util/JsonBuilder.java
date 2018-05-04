package com.booking.replication.augmenter.util;

//import com.booking.replication.applier.kafka.RowListMessage;

import com.booking.replication.augmenter.model.AugmentedRowImplementation;
import com.booking.replication.augmenter.model.AugmentedSchemaChangeEvent;
import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.augmenter.active.schema.augmented.active.schema.TableSchemaVersion;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Json Builder.
 */

public class JsonBuilder {

    private static final ObjectMapper om = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonBuilder.class);

    public static String augmentedRowToJson(AugmentedRowImplementation augmentedRow) {
        String json = null;
        try {
            json = om.writeValueAsString(augmentedRow);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

//    public static String rowListMessageToJSON(RowListMessage rowListMessage) {
//        String json = null;
//        try {
//            json = om.writeValueAsString(rowListMessage);
//        } catch (IOException e) {
//            LOGGER.error("ERROR: could not serialize RowListMessage object.", e);
//            System.exit(-1);
//        }
//        return json;
//    }
//
//    public static RowListMessage rowListMessageFromJSON(String jsonString) {
//        RowListMessage rowListMessageFrom = null;
//        try {
//            rowListMessageFrom = om.readValue(jsonString, RowListMessage.class);
//        } catch (IOException e) {
//            LOGGER.error("ERROR: could not deserialize RowListMessage object from jsonString" + jsonString, e);
//            System.exit(-1);
//        }
//        return rowListMessageFrom;
//    }

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

    public static String schemaTransitionSequenceToJson(HashMap<String, String> schemaTransitionSequence) {
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
