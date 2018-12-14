package com.booking.replication.controller;

import org.eclipse.jetty.server.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public abstract class WebServer implements Closeable {

    public enum ServerType {

        NONE {
            @Override
            protected WebServer newInstance(Map<String, Object> configuration) {
                return new DummyWebServer(configuration);
            }
        },

        JETTY {
            @Override
            protected WebServer newInstance(Map<String, Object> configuration) {
                return new JettyWebServer(configuration);
            }
        };

        protected abstract WebServer newInstance(Map<String, Object> configuration);
    }

    public interface Configuration {
        String TYPE = "webserver.type";
    }

    public static WebServer build(Map<String, Object> configuration) {
        return WebServer.ServerType.valueOf(
                configuration.getOrDefault(WebServer.Configuration.TYPE, ServerType.NONE.name()).toString()
        ).newInstance(configuration);
    }

    public abstract void start() throws IOException;

    public abstract void stop() throws IOException;

    public abstract Server getServer();

    @Override
    public void close() throws IOException {

    }
}
