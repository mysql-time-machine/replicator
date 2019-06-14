package com.booking.replication.commons.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import static com.google.common.math.Quantiles.percentiles;

public class QueuesMetricSet<T> implements MetricSet {

    final private Queue<T>[] queues;

    public QueuesMetricSet(Queue<T>[] queues) {
        this.queues = queues;
    }

    public Map<String, Metric> getMetrics() {
        Map<String, Metric> gauges = new HashMap<>();

        gauges.put("count", new QueueGauge<T, Integer>(this.queues, (s) -> s.length));
        gauges.put("sum",   new QueueGauge<T, Integer>(this.queues, (s) -> Arrays.stream(s).sum()));
        gauges.put("max",   new QueueGauge<T, Integer>(this.queues, (s) -> Arrays.stream(s).max().orElse(0)));
        gauges.put("min",   new QueueGauge<T, Integer>(this.queues, (s) -> Arrays.stream(s).min().orElse(0)));
        gauges.put("avg",   new QueueGauge<T, Double>(this.queues,  (s) -> Arrays.stream(s).average().orElse(0)));

        gauges.put("percentiles", (MetricSet)() -> {
            Map<String, Metric> percentileGauges = new HashMap<>();

            int[] percentiles = {10, 25, 50, 75, 90, 95, 99};

            Arrays.stream(percentiles).forEach((x) -> {
                percentileGauges.put("p"+x,
                        new QueueGauge<T, Double>(this.queues,
                                (s) -> percentiles().index(x).compute(s)
                        )
                );
            });

            return percentileGauges;
        });

        return gauges;
    }
}

class QueueGauge<T, K> implements Gauge<K> {

    final Function<int[], K> function;
    final private Queue<T>[] queues;

    QueueGauge(Queue<T>[] queues, Function<int[], K> function) {
        this.queues = queues;
        this.function = function;
    }

    public K getValue() {
        int[] sizes = getQueueSizes();
        return function.apply(sizes);
    }

    private int[] getQueueSizes() {

        if (queues == null || queues.length == 0) {
            return new int[0];
        }

        int[] sizes = new int[queues.length];

        for (int i = 0; i < queues.length; i++) {
            sizes[i] = queues[i].size();
        }

        return sizes;
    }
}