package edu.northwestern.ssa;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;

public class App {
    public static void main(String[] args) {
        String commonCrawlFilename = System.getenv("COMMON_CRAWL_FILENAME");

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofMinutes(30)).build())
                .build();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket("commoncrawl")
                .key(commonCrawlFilename)
                .build();
        ResponseInputStream<GetObjectResponse> ris = s3.getObject(request);

        try {
            ArchiveReader ar = WARCReaderFactory.get(commonCrawlFilename, ris, true);

            ElasticSearch es = new ElasticSearch();
            es.CreateIndex();

            ArrayList<String> docs = new ArrayList<>();
            for (ArchiveRecord r : ar) {
                String url = r.getHeader().getUrl();
                if (url == null) continue;

                byte[] rawData = IOUtils.toByteArray(r, r.available());
                String content = new String(rawData);
                content = content.substring(content.indexOf("\r\n\r\n") + 4);
                content = content.replace("\0", "");

                Document doc = Jsoup.parse(content);
                String text = doc.text(), title = doc.title();

                if (text == null) continue;
                JSONObject body = new JSONObject();
                body.put("title", title);
                body.put("txt", text);
                body.put("url", url);

                es.PostDocument(body);
            }

            es.close();
            s3.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
