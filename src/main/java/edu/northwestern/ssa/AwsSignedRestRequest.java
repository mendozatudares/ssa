package com.stevetarzia.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.StringInputStream;

import java.io.Closeable;
import java.io.IOException;

public class AwsSignedRestRequest implements Closeable {
    private Aws4SignerParams params;
    private Aws4Signer signer = Aws4Signer.create();
    private SdkHttpClient httpClient = ApacheHttpClient.builder().build();

    /** @param serviceName would be "es" for Elasticsearch */
    AwsSignedRestRequest(String serviceName) {
         params = Aws4SignerParams.builder()
                .awsCredentials(DefaultCredentialsProvider.create().resolveCredentials())
                .signingName(serviceName)
                .signingRegion(Region.US_EAST_2)
                .build();
    }

    /** @param path should not have a leading "/" */
    protected HttpExecuteResponse restRequest(SdkHttpMethod method, String host, String path)
            throws IOException {
        return restRequest(method, host, path, null);
    }

    protected HttpExecuteResponse restRequest(SdkHttpMethod method, String host, String path, JSONObject body)
            throws IOException {
        if (method.equals(SdkHttpMethod.GET) && body != null) {
            throw new IOException("GET request cannot have a body. Otherwise Aws4Signer will include the body in the " +
                    "signature calculation, but it will not be included in the request, leading to a 403 error back from AWS.");
        }
        SdkHttpFullRequest.Builder b = SdkHttpFullRequest.builder()
                .encodedPath(path)
                .host(host)
                .method(method)
                .protocol("https");
        if (body != null) {
            b.putHeader("Content-Type", "application/json; charset=utf-8");
            b.contentStreamProvider(() -> new StringInputStream(body.toString()));
        }
        SdkHttpFullRequest request = b.build();

        // now sign it
        SdkHttpFullRequest signedRequest = signer.sign(request, params);
        HttpExecuteRequest.Builder rb = HttpExecuteRequest.builder().request(signedRequest);
        // !!!: line below is necessary even though the contentStreamProvider is in the request.
        // Otherwise the body will be missing from the request and auth signature will fail.
        request.contentStreamProvider().ifPresent(c -> rb.contentStreamProvider(c));
        return httpClient.prepareRequest(rb.build()).call();
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
