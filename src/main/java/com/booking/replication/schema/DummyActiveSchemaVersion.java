package com.booking.replication.schema;

import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.table.TableSchemaVersion;

import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by edmitriev on 8/2/17.
 */
public class DummyActiveSchemaVersion implements ActiveSchemaVersion {
    @Override
    public void loadActiveSchema() throws SQLException {

    }

    @Override
    public String schemaTablesToJson() {
        return null;
    }

    @Override
    public String schemaCreateStatementsToJson() {
        return null;
    }

    @Override
    public String toJson() {
        return null;
    }

    @Override
    public TableSchemaVersion getTableSchemaVersion(String tableName) {
        return null;
    }

    @Override
    public AugmentedSchemaChangeEvent transitionSchemaToNextVersion(HashMap<String, String> schemaTransitionSequence, Long timestamp) throws SchemaTransitionException {
        return null;
    }

    @Override
    public void applyDDL(HashMap<String, String> sequence) throws SchemaTransitionException, SQLException {

    }
}
