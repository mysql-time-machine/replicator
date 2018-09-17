package com.booking.replication;

import java.io.IOException;

public interface ReplicatorIntegrationTest {

    public void doMySQLOps();

    public Object getExpected();

    public Object retrieveReplicatedData() throws IOException;

    public boolean retrievedEqualsExpected(Object expected, Object retrieved);

}
