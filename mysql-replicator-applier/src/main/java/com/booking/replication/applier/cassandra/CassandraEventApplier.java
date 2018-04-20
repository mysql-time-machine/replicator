package com.booking.replication.applier.cassandra;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.model.RawEvent;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.Map;
import java.util.Objects;

public class CassandraEventApplier implements EventApplier {
    public interface Configuration {
        String CONTACT_POINTS = "cassandra.contact.points";
        String PORT = "cassandra.port";
    }

    private final Cluster cluster;
    private final Session session;

    public CassandraEventApplier(Map<String, String> configuration) {
        String contactPoints = configuration.get(Configuration.CONTACT_POINTS);
        String port = configuration.getOrDefault(Configuration.PORT, "9042");

        Objects.requireNonNull(contactPoints, String.format("Configuration required: %s", Configuration.CONTACT_POINTS));
        Objects.requireNonNull(port, String.format("Configuration required: %s", Configuration.PORT));

        this.cluster = this.getCluster(contactPoints.split(";"), Integer.parseInt(port));
        this.session = this.cluster.connect();
    }

    private Cluster getCluster(String[] contactPoints, int port) {
        return Cluster.builder().addContactPoints(contactPoints).withPort(port).build();
    }

    @Override
    public void accept(RawEvent rawEvent) {
    }

    @Override
    public void close() {
        this.session.close();
        this.cluster.close();
    }
}
