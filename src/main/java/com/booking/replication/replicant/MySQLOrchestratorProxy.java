package com.booking.replication.replicant;

import com.booking.replication.Configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * MySQLOrchestratorProxy
 * Handles the calling of orchestrator http API in order to obtain
 * binlog file name and position that corresponds to Pseudo GTID
 * The request looks like:
 *  http request to => $orchestrator_api_url/find-binlog-entry/$active_replicant_host/3306/$full_pgtid_query
 *  gives result like:
 *  {
 *      Code: "OK",
 *      Message: "binlog.000007:104113458",
 *      Details: null
 *  }
 */
public class MySQLOrchestratorProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLOrchestratorProxy.class);

    @JsonDeserialize
    private static OrchestratorResponse orchestratorResponse = new OrchestratorResponse();

    public static class OrchestratorResponse {

        @JsonDeserialize
        @JsonProperty("Code")
        public String code;

        @JsonDeserialize
        @JsonProperty("Message")
        public String message;

        @JsonDeserialize
        @JsonProperty("Details")
        public String details;

    }

    public static String[] findBinlogEntry(
        String orchestratorAPIUserName,
        String orchestratorAPIPassword,
        String orchestratorAPIUrl,
        String fullQueryPseudoGTID,
        String mysqlHost,
        String port) throws Exception {

        String url = orchestratorAPIUrl
                + "/find-binlog-entry/"
                + mysqlHost
                + "/"
                + port
                + "/";

        fullQueryPseudoGTID = URLEncoder.encode(fullQueryPseudoGTID, "UTF-8");

        url += fullQueryPseudoGTID;
        URI uriGetPositionInfoFromPseudoGTID = URI.create(url);

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(uriGetPositionInfoFromPseudoGTID);
        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
            orchestratorAPIUserName,
            orchestratorAPIPassword
        );
        Header header = new BasicScheme(StandardCharsets.UTF_8).authenticate(creds , request, null);
        request.addHeader(header);
        HttpResponse response = client.execute(request);

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        LOGGER.info("Got orchestrator response: " + result.toString());

        ObjectMapper mapper = new ObjectMapper();
        orchestratorResponse =  mapper.readValue(result.toString(), OrchestratorResponse.class);
        if (orchestratorResponse.code.equals("OK")) {
            String[] binlogCoordinates = orchestratorResponse.message.split(":");
            return binlogCoordinates;
        } else {
            throw new Exception("Could not retrieve binlog information from the orchestrator for host " + mysqlHost);
        }
    }
}
