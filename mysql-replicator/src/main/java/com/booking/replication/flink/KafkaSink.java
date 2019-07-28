package com.booking.replication.flink;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Map;

public class KafkaSink extends RichSinkFunction<Collection<AugmentedEvent>> {

        private transient  Map<String, Object> configuration;

        private transient KafkaApplier kafkaApplier;

        public KafkaSink(Map<String, Object> configuration) {
            this.configuration = configuration;
            this.kafkaApplier = new KafkaApplier(configuration);
        }

        @Override
        public void invoke(Collection<AugmentedEvent> augmentedEvents, SinkFunction.Context context) throws Exception {
            kafkaApplier.apply(augmentedEvents);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

}
