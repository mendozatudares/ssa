package edu.northwestern.ssa.api;

import edu.northwestern.ssa.AwsSignedRestRequest;
import edu.northwestern.ssa.Config;

import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.utils.IoUtils;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/search")
public class Search {

    /** when testing, this is reachable at http://localhost:8080/api/search?query=hello */
    /*
    @GET
    public Response getMsg(@QueryParam("query") String q) throws IOException {
        JSONArray results = new JSONArray();
        results.put("hello world!");
        results.put(q);
        return Response.status(200).type("application/json").entity(results.toString(4))
                // below header is for CORS
                .header("Access-Control-Allow-Origin", "*").build();
    }
    */

    @GET
    public Response getQuery(
            @QueryParam("query") String q,
            @QueryParam("language") String l,
            @QueryParam("date") String d,
            @DefaultValue("10") @QueryParam("count") int c,
            @DefaultValue("0") @QueryParam("offset") int o) throws IOException {
        if (q == null)
            return Response.status(400).type("text/plain").entity("'query' is missing from the url.")
                    .header("Access-Control-Allow-Origin", "*")
                    .build();

        String elasticSearchHost = Config.getParam("ELASTIC_SEARCH_HOST");
        String elasticSearchIndex = Config.getParam("ELASTIC_SEARCH_INDEX");

        String query = "txt:(" + String.join(" AND ", q.split("\\s+")) + ")";
        if (l != null) query += " AND lang:" + l;
        if (d != null) query += " AND date:" + d;

        // https://www.baeldung.com/java-initialize-hashmap#1-usingcollectorstomap
        Map<String, String> queryParams = Stream.of(new String[][] {
                { "q", query },
                { "from", Integer.toString(o) },
                { "size", Integer.toString(c) }
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

        AwsSignedRestRequest request = new AwsSignedRestRequest("es");
        HttpExecuteResponse response = request.restRequest(
                SdkHttpMethod.GET,
                elasticSearchHost,
                elasticSearchIndex + "/_search",
                Optional.of(queryParams));

        if (response.responseBody().isPresent()) {
            AbortableInputStream responseBody = response.responseBody().get();
            JSONObject responseJSON = new JSONObject(IoUtils.toUtf8String(responseBody)).getJSONObject("hits");
            JSONArray hits = responseJSON.getJSONArray("hits");

            JSONObject results = new JSONObject();
            results.put("returned_results", hits.length());
            results.put("total_results", responseJSON.getJSONObject("total").getInt("value"));
            results.put("articles", new JSONArray());

            for(int i = 0; i < hits.length(); i++)
                results.getJSONArray("articles").put(i, hits.getJSONObject(i).getJSONObject("_source"));

            responseBody.close();
            return Response.status(200).type("application/json").entity(results.toString(4))
                    // below header is for CORS
                    .header("Access-Control-Allow-Origin", "*").build();
        }

        return Response.status(200).type("application/json")
                // below header is for CORS
                .header("Access-Control-Allow-Origin", "*").build();
    }
}
