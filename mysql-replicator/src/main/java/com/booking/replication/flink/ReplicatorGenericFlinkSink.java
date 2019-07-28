package com.booking.replication.flink;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Map;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.App;

import java.util.Collection;
import java.util.Map;

public class ReplicatorGenericFlinkSink extends RichSinkFunction<Collection<AugmentedEvent>> {

        private transient Applier applier;

        public ReplicatorGenericFlinkSink(Map<String, Object> configuration) {
            System.out.println("Created applier");
            this.applier = Applier.build(configuration);
        }

        @Override
        public void invoke(Collection<AugmentedEvent> augmentedEvents, SinkFunction.Context context) throws Exception {

            applier.apply(augmentedEvents);
        }

    }
