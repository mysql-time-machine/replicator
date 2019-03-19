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
            protected Supplier newInstance(Map<String, Object> configuration) {
                return new BinaryLogSupplier(configuration);
            }
        };

        protected abstract Supplier newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "supplier.type";
    }

    String getGTIDSet();

    void onEvent(Consumer<RawEvent> consumer);

    void onException(Consumer<Exception> handler);

    void start(Checkpoint checkpoint) throws IOException;

    void connect(Checkpoint checkpoint) throws IOException;

    void disconnect() throws IOException;

    void stop() throws IOException;

    @SuppressWarnings("unchecked")
    static Supplier build(Map<String, Object> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.BINLOG.name()).toString()
        ).newInstance(configuration);
    }
}
