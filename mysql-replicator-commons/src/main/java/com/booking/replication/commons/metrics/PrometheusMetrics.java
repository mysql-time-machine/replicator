package com.booking.replication.commons.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import java.io.IOException;
import java.util.Map;

public class PrometheusMetrics extends Metrics<ScheduledReporter> {


    public PrometheusMetrics(Map<String, Object> configuration, Server server) {
        super(configuration);
        assert server != null;
        ServletHandler handler = (ServletHandler) server.getHandler();
        handler.addServletWithMapping(MetricsServlet.class, "/metrics");

        // Add metrics about CPU, JVM memory etc.
        DefaultExports.initialize();

        CollectorRegistry.defaultRegistry.register(new DropwizardExports(this.getRegistry()));
    }


    @Override
    protected ScheduledReporter getReporter(Map<String, Object> configuration, MetricRegistry registry) {
        return null;
    }

    @Override
    public void close() throws IOException {
        // Do nothing, as here we just have to stop the server where prometheus servlet is running.
    }

    @Override
    public void incrementCounter(String name, long val) {
        // meter is translated to counter
        this.getRegistry().meter(name).mark(val);
    }

}
