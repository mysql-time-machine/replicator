package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.google.code.or.binlog.StatusVariable;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.variable.status.QTimeZoneCode;
import com.google.code.or.common.glossary.column.StringColumn;

import java.util.HashMap;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEventQuery extends RawBinlogEvent {
    public RawBinlogEventQuery(Object event) throws Exception {
        super(event);
    }

    public String getSql() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getSql().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getSql();
        }
    }

    public void setSql(String sql) {
        if (USING_DEPRECATED_PARSER) {
            ((QueryEvent) this.getBinlogEventV4()).setSql(
                    StringColumn.valueOf(sql.getBytes())
            );
        }
        else {
            ((QueryEventData) this.getBinlogConnectorEvent().getData()).setSql(sql);
        }
    }

    public String getDatabaseName() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getDatabaseName().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getDatabase();
        }
    }

    public boolean hasTimezoneOverride() {
        if (USING_DEPRECATED_PARSER) {
            for (StatusVariable av : ((QueryEvent) this.getBinlogEventV4()).getStatusVariables()) {
                // handle timezone overrides during schema changes
                if (av instanceof QTimeZoneCode) {
                    return true;
                } else {
                    return false;
                }
            }
            return false;
        }
        else {
            // TODO: bin log connector currently does not support status variables
            return false;
        }
    }

    public HashMap<String,String> getTimezoneOverrideCommands() {

        HashMap<String,String> sqlCommands = new HashMap<>();

        if (USING_DEPRECATED_PARSER) {


            for (StatusVariable av : ((QueryEvent) this.getBinlogEventV4()).getStatusVariables()) {

                QTimeZoneCode tzCode = (QTimeZoneCode) av;

                String timezone = tzCode.getTimeZone().toString();
                String timezoneSetCommand = "SET @@session.time_zone='" + timezone + "'";
                String timezoneSetBackToSystem = "SET @@session.time_zone='SYSTEM'";

                sqlCommands.put("timezonePre", timezoneSetCommand);
                sqlCommands.put("timezonePost", timezoneSetBackToSystem);
            }

            return sqlCommands;
        }
        else {
            // TODO: bin log connector currently does not support status variables
            return sqlCommands;
        }
    }

    public void setTimestamp(long timestamp) {
        this.overrideTimestamp(timestamp);
    }
}
