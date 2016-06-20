package com.booking.replication.util;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.table.TableSchema;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.util.MySQLConstants;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * Json Builder.
 */

public class JsonBuilder {

    private static final ObjectMapper om = new ObjectMapper();

    public String binlogEventV4ToJson(BinlogEventV4 event) {

        String json = null;
        try {
            switch (event.getHeader().getEventType()) {

                // todo: make this nicer - currently it is just a quick fix since jackson cant parse tableMapEvent
                case MySQLConstants.TABLE_MAP_EVENT:
                    json =  om.writeValueAsString(((TableMapEvent) event).toString());
                    break;

                default:
                    json = om.writeValueAsString(event);
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
            json = om.writeValueAsString(augmentedRow);
        } catch (IOException e) {
            System.out.println("ERROR: could not serialize event");
            e.printStackTrace();
        }
        return json;
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

    public static String schemaVersionTablesToJson(HashMap<String, TableSchema> schemaVersionTables) {
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
