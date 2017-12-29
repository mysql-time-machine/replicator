package com.booking.replication.applier;

import com.booking.replication.Metrics;
import com.codahale.metrics.Gauge;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

public class KafkaMetricsCollector implements MetricsReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricsCollector.class);

    private static ConcurrentHashMap<String, KafkaMetricGauge> monitored = new ConcurrentHashMap<>();

    private KafkaMetricGauge kafkaGauge;

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric: metrics) {
            String saName = sanitizeName(metric.metricName());
            if (!monitored.containsKey(saName)) {
                LOGGER.debug("Adding kafka metric: " + saName);
                kafkaGauge = new KafkaMetricGauge(metric);
                Metrics.registry.register(name("Kafka", saName), kafkaGauge);
                monitored.put(saName, kafkaGauge);
            } else {
                LOGGER.debug("Changing kafka metric: " + saName);
                monitored.get(saName).setMetric(metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        String saName = sanitizeName(metric.metricName());
        if (!monitored.containsKey(saName)) {
            LOGGER.debug("Adding kafka metric: " + saName);
            kafkaGauge = new KafkaMetricGauge(metric);
            Metrics.registry.register(name("Kafka", saName), kafkaGauge);
            monitored.put(saName, kafkaGauge);
        } else {
            LOGGER.debug("Changing kafka metric: " + saName);
            monitored.get(saName).setMetric(metric);
        }
    }

    private static class KafkaMetricGauge implements Gauge<Double> {
        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricGauge.class);

        private KafkaMetric metric;

        KafkaMetricGauge(KafkaMetric metric) {
            this.metric = metric;
        }

        public void setMetric(KafkaMetric metric) {
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

    @Override
    public void metricRemoval(KafkaMetric var1) {}
}
