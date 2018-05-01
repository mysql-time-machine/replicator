package com.booking.replication.schema.column.types;

import com.booking.replication.Configuration;

/**
 * Created by bdevetak on 3/14/18.
 */
public class TypeConversionRules {

    private final boolean stringifyNull;

    public TypeConversionRules(Configuration configuration) {
        stringifyNull = configuration.getConverterStringifyNull();
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
