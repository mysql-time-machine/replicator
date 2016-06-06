package com.booking.replication.metrics;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.util.Duration;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by rmirica on 06/06/16.
 */
public class GraphiteReporter extends MetricsReporter {

    private static final String type = "graphite";

    private ScheduledReporter reporter;
    private Duration frequency = Duration.parse("10 seconds");

    public ScheduledReporter getReporter() { return reporter; }
    public Duration getFrequency() { return frequency; }

    public GraphiteReporter(Configuration conf) {
        Configuration.MetricsConfig.ReporterConfig metricConf = conf.getReporterConfig(GraphiteReporter.type);

        frequency = conf.getReportingFrequency();

        String[] urlSplit = metricConf.url.split(":");
        String hostName = urlSplit[0];
        int port = 3002;
        if(urlSplit.length > 1) {
            port = Integer.parseInt(urlSplit[1]);
        }

        reporter = com.codahale.metrics.graphite.GraphiteReporter
                .forRegistry(Metrics.registry)
                .prefixedWith(metricConf.namespace)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(new Graphite(new InetSocketAddress(hostName, port)));
    }
}
