package com.booking.replication.binlog;

import com.booking.replication.Configuration;
import com.booking.replication.binlog.BinlogEventParserProviderCode;
import com.booking.replication.pipeline.PipelinePosition;
import com.google.code.or.OpenReplicator;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

/**
 * Created by bosko on 5/13/17.
 */
public class BinlogEventParserProviderFactory {

    public static Object getBinlogEventParserProvider(
            int serverId,
            int BINLOG_EVENT_PARSER_PROVIDER_CODE,
            Configuration configuration,
            PipelinePosition pipelinePosition) throws Exception {

        if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR ) {

            OpenReplicator openReplicator = new OpenReplicator();

            // config
            openReplicator.setUser(configuration.getReplicantDBUserName());
            openReplicator.setPassword(configuration.getReplicantDBPassword());
            openReplicator.setPort(configuration.getReplicantPort());

            // host pool
            openReplicator.setHost(pipelinePosition.getCurrentReplicantHostName());
            openReplicator.setServerId(serverId);

            // position
            openReplicator.setBinlogPosition(pipelinePosition.getCurrentPosition().getBinlogPosition());
            openReplicator.setBinlogFileName(pipelinePosition.getCurrentPosition().getBinlogFilename());

            // disable lv2 buffer
            openReplicator.setLevel2BufferSize(-1);

           return  openReplicator;
        }
        else if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.SHYIKO) {

            BinaryLogClient client = new BinaryLogClient(
                pipelinePosition.getCurrentReplicantHostName(),
                configuration.getReplicantPort(),
                configuration.getReplicantDBUserName(),
                configuration.getReplicantDBPassword()
            );

            client.setServerId(serverId);

            client.setBinlogPosition(pipelinePosition.getCurrentPosition().getBinlogPosition());

            client.setBinlogFilename(pipelinePosition.getCurrentPosition().getBinlogFilename());

            return client;
        }
        else {
            throw new Exception("Unsupported parser exception");
        }
    }
}
