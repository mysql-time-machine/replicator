package com.booking.replication.supplier;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public interface Supplier {
    enum Type {
        BINLOG {
            @Override
            protected Supplier newInstance(Map<String, String> configuration) {
                return new BinaryLogSupplier(configuration);
            }
        };

        protected abstract Supplier newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "supplier.type";
    }

    void onEvent(Consumer<RawEvent> consumer);

    void start(Checkpoint checkpoint) throws IOException;

    void stop() throws IOException;

    @SuppressWarnings("unchecked")
    static Supplier build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.BINLOG.name())
        ).newInstance(configuration);
    }
}
