package com.booking.replication.commons.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import org.eclipse.jetty.server.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

import java.util.Map;

public abstract class Metrics<CloseableReporter extends Closeable & Reporter> implements Closeable {
    private static final Logger LOG = LogManager.getLogger(Metrics.class);

    public enum Type {

        CONSOLE {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration, Server server) {
                if (instance == null) {
                    instance =  new ConsoleMetrics(configuration);
                }
                return instance;
            }
        },
        JMX {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration, Server server) {
                if (instance == null) {
                    instance =  new JMXMetrics(configuration);
                }
                return instance;
            }
        },
        GRAPHITE {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration, Server server) {
                if (instance == null) {
                    instance =  new GraphiteMetrics(configuration);
                }
                return instance;
            }
        },PROMETHEUS {
            @Override
            protected Metrics<?> newInstance(Map<String, Object> configuration, Server server) {
                if (instance == null) {
                    instance =  new PrometheusMetrics(configuration, server);
                }
                return instance;
            }
        };

        private static Metrics<?> instance;

        protected abstract Metrics<?> newInstance(Map<String, Object> configuration, Server server);

        public Metrics<?> getInstance(){
            if(instance == null){
                Metrics.LOG.fatal("Metrics.build(configuration) should have been called during starting the replicator");
            }
            return  instance;
        }
    }

    public interface Configuration {
        String TYPE = "metrics.applier.type";
        String BASE_PATH = "metrics.applier.base_path";
        String MYSQL_SCHEMA = "mysql.schema";
    }

    private final MetricRegistry registry;
    private final CloseableReporter reporter;
    private String basePath;

    public Metrics(Map<String, Object> configuration) {
        this.registry = new MetricRegistry();
        this.reporter = this.getReporter(configuration, this.registry);
        String base = String.valueOf(configuration.getOrDefault(Configuration.BASE_PATH, "replicator"));
        this.basePath = MetricRegistry.name(base, String.valueOf(configuration.getOrDefault(Configuration.MYSQL_SCHEMA, "db")));
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    public void incrementCounter(String name, long val) {
        this.registry.counter(name).inc(val);
    }

    public void updateMeter(String name, long val) {
        this.registry.meter(name).mark(val);
    }

    public <T extends Metric> T register(String name, T metric) {

        final String fullName = MetricRegistry.name(basePath, name);

        if (this.registry.remove(fullName)) {
            LOG.warn(String.format("Metric %s already registered.", fullName));
        }

        T response = this.registry.register(fullName, metric);

        LOG.info(String.format("Metric %s registered", fullName));

        return response;
    }

    @Override
    public void close() throws IOException  {
        this.reporter.close();
    }

    public String basePath(){
        return basePath;
    }

    protected abstract CloseableReporter getReporter(Map<String, Object> configuration, MetricRegistry registry);

    public static Metrics<?> build(Map<String, Object> configuration, Server server) {
        return Metrics.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name()).toString()
        ).newInstance(configuration, server);
    }

    public static Metrics<?> getInstance(Map<String, Object> configuration){
        return Metrics.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name()).toString()
        ).getInstance();
    }
 }
