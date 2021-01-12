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
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class App {
    public static void main(String[] args) {
        String commonCrawlFilename = System.getenv("COMMON_CRAWL_FILENAME");
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofMinutes(30)).build())
                .build();

        if (commonCrawlFilename == null || commonCrawlFilename.equals("")) {
            // No specified common crawl file to download. Download the latest file
            LocalDate date = LocalDate.now();
            ListObjectsRequest listObjects = getListObjectsRequest(date.getYear(), date.getMonthValue());
            ListObjectsResponse res = s3.listObjects(listObjects);

            int i = 3;
            while (res.contents().size() == 0 && i-- > 0) {
                date = date.minusMonths(1);
                listObjects = getListObjectsRequest(date.getYear(), date.getMonthValue());
                res = s3.listObjects(listObjects);
            }

            List<S3Object> s3Objects = res.contents().stream()
                    .sorted(Comparator.comparing(S3Object::lastModified))
                    .collect(Collectors.toList());

            commonCrawlFilename = s3Objects.get(s3Objects.size() - 1).key();
        }

        GetObjectRequest request = GetObjectRequest.builder()
            .bucket("commoncrawl")
            .key(commonCrawlFilename)
            .build();
        ResponseInputStream<GetObjectResponse> ris = s3.getObject(request);

        try {
            ArchiveReader ar = WARCReaderFactory.get(commonCrawlFilename, ris, true);

            ElasticSearch es = new ElasticSearch();
            es.CreateIndex();

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
            s3.close();
        }
    }

    private static ListObjectsRequest getListObjectsRequest(int year, int month) {
        return ListObjectsRequest.builder()
                .bucket("commoncrawl")
                .prefix(String.format("crawl-data/CC-NEWS/%04d/%02d/", year, month))
                .build();
    }
}
