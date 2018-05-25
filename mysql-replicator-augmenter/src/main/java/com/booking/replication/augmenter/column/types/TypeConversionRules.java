package com.booking.replication.augmenter.column.types;

import java.util.HashMap;

public class TypeConversionRules {

    interface Configuration {
        String STRINGIFY_NULL = "augmenter.stringify.null";
    }

    private final boolean stringifyNull;

    public TypeConversionRules(HashMap<String,String> configuration) {
        stringifyNull = Boolean.getBoolean(configuration.get(Configuration.STRINGIFY_NULL));
    }

    public boolean isStringifyNull() {
        return stringifyNull;
    }

    public String blobToHexString( byte [] raw ) {
        if ( raw == null ) {
            return stringifyNull ? "NULL" : null;
        }
        final StringBuilder hex = new StringBuilder( 2 * raw.length );
        for ( final byte b : raw ) {
            int ivalue = b & 0xFF;
            if (ivalue < 16 ) {
                hex.append("0").append(Integer.toHexString(ivalue).toUpperCase());
            } else {
                hex.append(Integer.toHexString(ivalue).toUpperCase());
            }
        }
        return hex.toString();
    }
}
