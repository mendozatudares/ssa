package edu.northwestern.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.ArrayList;
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

	public void PostDocument(JSONObject doc) throws IOException {
		restRequest(SdkHttpMethod.POST,
				elasticSearchHost,
				elasticSearchIndex + "/_doc/",
				Optional.empty(),
				Optional.of(doc)).responseBody().get().close();
	}
}
