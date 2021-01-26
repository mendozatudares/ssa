package edu.northwestern.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElasticSearch extends AwsSignedRestRequest {
	private final String elasticSearchHost;
	private final String elasticSearchIndex;

	ElasticSearch() {
		super("es");
		elasticSearchHost = System.getenv("ELASTIC_SEARCH_HOST");
		elasticSearchIndex = System.getenv("ELASTIC_SEARCH_INDEX");
	}

	public void CreateIndex() throws IOException {
		restRequest(SdkHttpMethod.PUT,
				elasticSearchHost,
				elasticSearchIndex,
				Optional.empty()).responseBody().get().close();
	}

	public void PutDocument(JSONObject doc) throws IOException {
		String id = String.valueOf(doc.get("url").hashCode());
		Map<String, String> query = new HashMap<>();
		query.put("op_type", "create");

		restRequest(SdkHttpMethod.PUT,
				elasticSearchHost,
				elasticSearchIndex + "/_doc/" + id,
				Optional.of(query),
				Optional.of(doc)).responseBody().get().close();
	}
}
