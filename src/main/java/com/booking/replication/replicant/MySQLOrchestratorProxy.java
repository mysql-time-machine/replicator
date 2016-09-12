package com.booking.replication.replicant;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.*;

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

    @JsonDeserialize
    public OrchestratorResponse orchestratorResponse = new OrchestratorResponse();

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

    public static void getOrchestratorResponse(
        String orchestratorAPIUrl,
        String fullQueryPseudoGTID,
        String mysqlHost,
        String port) throws IOException {

        //        String url = orchestratorAPIUrl
        //                + "/find-binlog-entry/"
        //                + mysqlHost
        //                + "/"
        //                + port
        //                + "/"
        //                + fullQueryPseudoGTID;

        String url = "http://localhost:3000/api2/find-binlog-entry/cccc";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        // add request header
        request.addHeader("User-Agent", "Replicator");
        HttpResponse response = client.execute(request);

        System.out.println("Response Code : "
                + response.getStatusLine().getStatusCode());

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        System.out.println(result);

    }
}
