package com.booking.replication.controller;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import java.io.IOException;
import java.util.Map;

public class JettyWebServer extends WebServer {
    private final Server server;

    public JettyWebServer(Map<String, Object> configuration) {
        this.server = new Server(9999);

        ServletHandler servletHandler = new ServletHandler();
        server.setHandler(servletHandler);

        servletHandler.addServletWithMapping(HealthCheckServlet.class, "/healthcheck");
    }

    @Override
    public void start() throws IOException {
        try {
            this.server.start();
        } catch (Exception e) {
            throw new IOException("Error while starting server", e);
        }
    }

    @Override
    public void stop() throws IOException {
        try {
            this.server.stop();
        } catch (Exception e) {
            throw new IOException("Error while stopping server", e);
        }
    }

    @Override
    public Server getServer() {
        return this.server;
    }

    @Override
    public void close() throws IOException {
        try {
            this.stop();
        } catch (Exception e) {
            throw new IOException("Error stopping server", e);
        }
    }
}
