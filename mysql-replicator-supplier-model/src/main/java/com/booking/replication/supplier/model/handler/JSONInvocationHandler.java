package com.booking.replication.supplier.model.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class JSONInvocationHandler extends MapInvocationHandler {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };

    public JSONInvocationHandler(ObjectMapper mapper, byte[] data) throws IOException {
        super(mapper.readValue(data, JSONInvocationHandler.TYPE_REFERENCE));
    }

    public JSONInvocationHandler(byte[] data) throws IOException {
        this(new ObjectMapper(), data);
    }
}
