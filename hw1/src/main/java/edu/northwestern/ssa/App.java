package edu.northwestern.ssa;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
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

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            int prefixLength = "crawl-data/CC-NEWS/XXXX/XX/CC-NEWS-".length();
            List<S3Object> s3Objects = res.contents().stream()
                    .sorted(Comparator.comparing(object -> dateFormat.parse(
                            object.key(), new ParsePosition(prefixLength))))
                    .collect(Collectors.toList());

            commonCrawlFilename = s3Objects.get(s3Objects.size() - 1).key();
        }

        GetObjectRequest request = GetObjectRequest.builder()
            .bucket("commoncrawl")
            .key(commonCrawlFilename)
            .build();
        ResponseInputStream<GetObjectResponse> ris = s3.getObject(request);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat stringFormat = new SimpleDateFormat("yyyy-MM-dd");
        int prefixLength = "crawl-data/CC-NEWS/XXXX/XX/CC-NEWS-".length();
        String date = stringFormat.format(dateFormat.parse(commonCrawlFilename, new ParsePosition(prefixLength)));

        try {
            ArchiveReader ar = WARCReaderFactory.get(commonCrawlFilename, ris, true);

            ElasticSearch es = new ElasticSearch();
            try {
                es.CreateIndex();
            } catch (IOException ignored) {}

            int count = 0;
            String batchSizeString = System.getenv("BATCH_SIZE");
            int batchSize = (batchSizeString != null) ? Integer.parseInt(batchSizeString) : 500;
            JSONArray body = new JSONArray();
            for (ArchiveRecord r : ar) {
                String url = r.getHeader().getUrl();
                if (url == null) continue;

                // Read and clean content. If none found, continue
                String content = new String(IOUtils.toByteArray(r, r.available()));
                content = content.substring(content.indexOf("\r\n\r\n") + 4);
                content = content.replace("\0", "");
                if (content.equals("")) continue;

                // Parse content with Jsoup for title and text
                Document doc = Jsoup.parse(content);
                String title = doc.title(), text = doc.text();
                if (text.equals("") || title.equals(""))
                    continue;

                // Add document to batch, use following format
                // { "index" : { "_id" : "1" } }
                // { "title" : "value1", "txt": "value2", "url": "value3" ... }
                JSONObject source = new JSONObject();
                source.put("title", title);
                source.put("txt", text);
                source.put("url", url);
                source.put("date", date);

                JSONObject meta = new JSONObject();
                meta.put("_id", String.valueOf(url.hashCode()));
                JSONObject action = new JSONObject();
                action.put("index", meta);

                body.put(action);
                body.put(source);

                // Check if a new batch is ready to be posted
                if (++count < batchSize) continue;
                count = 0;

                try {
                    es.BulkPutDocuments(body);
                    body = new JSONArray();
                    continue;
                } catch (IOException ignored) {}
                try {
                    es.BulkPutDocuments(body);
                    body = new JSONArray();
                } catch (IOException e) {
                    System.out.println(e.toString());
                    System.out.println("Bulk index failed");
                }
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
                .prefix(String.format("crawl-data/CC-NEWS/%04d/%02d/CC-NEWS-", year, month))
                .build();
    }
}
