package com.booking.replication.util;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.table.TableSchema;
import com.google.code.or.binlog.BinlogEventV4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.util.MySQLConstants;

import java.io.IOException;
import java.util.HashMap;

/**
 * JsonBuilder
 */

public class JsonBuilder {

    public String binlogEventV4ToJson(BinlogEventV4 event) {

        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            switch (event.getHeader().getEventType()) {

                // todo: make this nicer - currently it is just a quick fix since jackson cant parse tableMapEvent
                case MySQLConstants.TABLE_MAP_EVENT:
                    json =  mapper.writeValueAsString(((TableMapEvent) event).toString());
                    break;

                default:
                    json = mapper.writeValueAsString(event);
                    break;
            }
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event type " + event.getHeader().getEventType());
        }
        return json;
    }

    public static String dataEventToJson(AugmentedRow augmentedRow) {

        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(augmentedRow);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaVersionToJson(ActiveSchemaVersion activeSchemaVersion) {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            //json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(activeSchemaVersion);
            json = mapper.writeValueAsString(activeSchemaVersion);

        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaChangeEventToJson(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent) {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(augmentedSchemaChangeEvent);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaTransitionSequenceToJson(HashMap<String,String> schemaTransitionSequence) {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(schemaTransitionSequence);

        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaVersionTablesToJson(HashMap<String, TableSchema> schemaVersionTables) {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(schemaVersionTables);

        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

    public static String schemaCreateStatementsToJson(HashMap<String, String> schemaCreateStatements) {
        String json = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(schemaCreateStatements);

        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
    }

}
