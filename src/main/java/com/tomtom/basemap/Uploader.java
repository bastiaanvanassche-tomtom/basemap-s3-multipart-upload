package com.tomtom.basemap;

public interface Uploader {

    void multipartUploadWithS3Client(String filePath, String bucketName, String key, String region, long chunkSize);
}
