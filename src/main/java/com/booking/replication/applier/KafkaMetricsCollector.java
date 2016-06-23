package com.booking.replication.applier;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;

import com.codahale.metrics.Gauge;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaMetricsCollector implements MetricsReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricsCollector.class);

    private static ConcurrentHashMap<KafkaMetric, Boolean> monitored = new ConcurrentHashMap<>();

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric: metrics) {
            if (!monitored.containsKey(metric)) {
                monitored.put(metric, true);
                LOGGER.debug("Adding kafka metric: " + sanitizeName(metric.metricName()));
                Metrics.registry.remove(name("Kafka", sanitizeName(metric.metricName())));
                Metrics.registry.register(name("Kafka", sanitizeName(metric.metricName())), new KafkaMetricGauge(metric));
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (!monitored.containsKey(metric)) {
            monitored.put(metric, true);
            LOGGER.debug("Adding kafka metric: " + sanitizeName(metric.metricName()));
            Metrics.registry.remove(name("Kafka", sanitizeName(metric.metricName())));
            Metrics.registry.register(name("Kafka", sanitizeName(metric.metricName())), new KafkaMetricGauge(metric));
        }
    }

    private static class KafkaMetricGauge implements Gauge<Double> {
        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricGauge.class);

        private KafkaMetric metric;

        KafkaMetricGauge(KafkaMetric metric) {
            this.metric = metric;
        }

        @Override
        public Double getValue() {
            return metric.value();
        }
    }

    private String sanitizeName(MetricName name) {
        StringBuilder result = new StringBuilder().append(name.group()).append('.');
        for (Map.Entry<String, String> tag : name.tags().entrySet()) {
            if (tag.getKey().equals("client-id")) {
                continue;
            }
            result.append(tag.getValue()).append('.');
        }
        return result.append(name.name()).toString().replace(' ', '_').replace("\\.", "_");
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
