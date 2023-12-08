package com.tomtom.basemap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class Uploader {
    private final Logger LOGGER = LoggerFactory.getLogger(Uploader.class);

    public void multipartUploadWithS3Client(String filePath, String bucketName, String key, String region, int chunkSize) {
        assert chunkSize >= 5 * 1024 * 1024;
        assert chunkSize <= 5 * 1024 * 1024 * 1024;

        S3Client s3Client = S3Client.builder()
                .region(Region.of(region))
                .forcePathStyle(true)
                .build();
        // Initiate the multipart upload.
        CreateMultipartUploadResponse createMultipartUploadResponse =
                s3Client.createMultipartUpload(b -> b
                        .bucket(bucketName)
                        .key(key));
        String uploadId = createMultipartUploadResponse.uploadId();
        LOGGER.info("UploadId = " + uploadId);
        // Upload the parts of the file.
        int partNumber = 1;
        List<CompletedPart> completedParts = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.allocate(chunkSize); // 5 MB byte buffer

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long fileSize = file.length();
            int position = 0;
            while (position < fileSize) {
                file.seek(position);
                int read = file.getChannel().read(bb);

                bb.flip(); // Swap position and limit before reading from the buffer.
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build();

                UploadPartResponse partResponse = s3Client.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromByteBuffer(bb));

                CompletedPart part = CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(partResponse.eTag())
                        .build();
                completedParts.add(part);

                bb.clear();
                position += read;
                partNumber++;
            }
        } catch (IOException e) {
            final AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder().uploadId(uploadId).build();
            s3Client.abortMultipartUpload(abortRequest);
            throw new RuntimeException("Multipart Upload failed", e);
        }

        // Complete the multipart upload.
        s3Client.completeMultipartUpload(b -> b
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
    }

}
