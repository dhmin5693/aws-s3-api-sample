import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

public class Example {

    private static String accessKey;
    private static String secretKey;
    private static String bucketName;

    public static void main(String[] args) throws Exception {

        try {
            initS3Properties();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("No properties file in classpath");
            return;
        }

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.AP_NORTHEAST_2)
                .build();

        listingBucket(s3);

        String key1 = uploadLennaToS3(s3);
        String key2 = uploadLennaToS3Folder(s3);
        updateImage(s3, key2);
        listingFileObject(s3);
        download(s3, key1);
        deleteAll(s3);
    }

    private static void initS3Properties() throws IOException {
        Properties properties = new Properties();

        InputStream inputStream = (Example.class).getResourceAsStream("aws.properties");
        if (inputStream == null) {
            inputStream = (Example.class).getResourceAsStream("aws-default.properties");
        }

        properties.load(inputStream);

        accessKey = properties.getProperty("access_key");
        secretKey = properties.getProperty("secret_key");
        bucketName = properties.getProperty("bucket_name");
    }

    private static void listingBucket(AmazonS3 s3) {
        System.out.println("Listing buckets");
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(bucket.getName());
        }
    }

    private static String uploadLennaToS3(AmazonS3 s3) throws URISyntaxException {
        String objectKey = "lenna-" + UUID.randomUUID();
        System.out.println("Uploading a new Lenna to S3");
        s3.putObject(new PutObjectRequest(bucketName, objectKey, createLennaImageFile()));
        return objectKey;
    }

    private static String uploadLennaToS3Folder(AmazonS3 s3) throws URISyntaxException {
        String objectKey = "myFolder/lenna-" + UUID.randomUUID();
        System.out.println("Uploading a new Lenna to S3");
        s3.putObject(new PutObjectRequest(bucketName, objectKey, createLennaImageFile()));
        return objectKey;
    }

    private static void updateImage(AmazonS3 s3, String objectKey) throws URISyntaxException {
        System.out.println("Updating the lenna to other image.");
        s3.putObject(new PutObjectRequest(bucketName, objectKey, createS3LogoImageFile()));
    }

    private static void listingFileObject(AmazonS3 s3) {
        listing(s3, "lenna");
        listing(s3, "myFolder");
    }

    private static void listing(AmazonS3 s3, String prefix) {
        System.out.println("Listing objects (" + prefix + ")");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(prefix));

        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("key: " + summary.getKey() + ", size: " + summary.getSize());
        }
    }

    private static void download(AmazonS3 s3, String objectKey) {
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, objectKey));
        System.out.println("Content-Type of Lenna: "  + object.getObjectMetadata().getContentType());
    }

    private static void deleteAll(AmazonS3 s3) {
        System.out.println("Deleting all objects");

        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName));

        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("delete key: " + summary.getKey());
            s3.deleteObject(bucketName, summary.getKey());
        }
    }

    private static File createLennaImageFile() throws URISyntaxException {
        URL url = (Example.class).getClassLoader().getResource("Lenna.png");
        return Paths.get(url.toURI()).toFile();
    }

    private static File createS3LogoImageFile() throws URISyntaxException {
        URL url = (Example.class).getClassLoader().getResource("s3logo.png");
        return Paths.get(url.toURI()).toFile();
    }
}