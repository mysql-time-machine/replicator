package com.booking.replication.controller;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import java.io.IOException;
import java.util.Map;

public class DummyWebServer extends WebServer {

    public DummyWebServer(Map<String, Object> configuration) {

    }

    @Override
    public void start() throws IOException {
        // noop
    }

    @Override
    public void stop() throws IOException {
        // noop
    }

    @Override
    public Server getServer() {
        return null;
    }
}
