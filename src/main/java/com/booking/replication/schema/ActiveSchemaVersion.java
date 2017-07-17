package com.booking.replication.schema;

import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.table.TableSchemaVersion;

import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by edmitriev on 8/2/17.
 */
public interface ActiveSchemaVersion {
    void loadActiveSchema() throws SQLException;

    String schemaTablesToJson();

    String schemaCreateStatementsToJson();

    String toJson();

    HashMap<String, TableSchemaVersion> getActiveSchemaTables();

    HashMap<String, String> getActiveSchemaCreateStatements();

    AugmentedSchemaChangeEvent transitionSchemaToNextVersion(HashMap<String, String> schemaTransitionSequence, Long timestamp)
            throws SchemaTransitionException;

    void applyDDL(HashMap<String, String> sequence)
                    throws SchemaTransitionException, SQLException;
}
