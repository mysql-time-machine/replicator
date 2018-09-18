package com.booking.replication;

import com.booking.replication.commons.services.ServicesControl;

import java.io.IOException;

public interface ReplicatorIntegrationTest {

    public void doMySQLOps(ServicesControl mysqlReplicant);

    public Object getExpected();

    public Object retrieveReplicatedData() throws IOException;

    public boolean retrievedEqualsExpected(Object expected, Object retrieved);

}
