package com.tomtom.basemap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Uploader {
    private final Logger LOGGER = LoggerFactory.getLogger(Uploader.class);

    public void multipartUploadWithS3Client(String filePath, String bucketName, String key, String region, int chunkSize) {
        assert chunkSize >= 5 * 1024 * 1024;
        assert chunkSize <= 5 * 1024 * 1024 * 1024;

        final long fileLength = new File(filePath).length();
        LOGGER.info("File length {}", fileLength);

        int noChunks = (int) (fileLength % chunkSize == 0 ? fileLength / chunkSize : fileLength / chunkSize + 1);
        LOGGER.info("No chunks {} for chunk size {}", noChunks, chunkSize);

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

        final ExecutorService pool = Executors.newFixedThreadPool(1);
        final CompletionService<CompletedPart> service = new ExecutorCompletionService<>(pool);

        IntStream
                .range(1, noChunks + 1)
                .mapToObj(i -> createPart(filePath, i, chunkSize, bucketName, key, s3Client, uploadId)
                ).forEach(c -> service.submit(c));

        final List<CompletedPart> completedParts = IntStream.range(0, noChunks).mapToObj(i -> {
            try {
                final Future<CompletedPart> take = service.take();
                final CompletedPart completedPart = take.get();
                return completedPart;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        // Complete the multipart upload.
        s3Client.completeMultipartUpload(b -> b
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
    }

    private Callable<CompletedPart> createPart(String fileName,
                                               int partNumber,
                                               int chunkSize,
                                               String bucketName,
                                               String key,
                                               S3Client s3Client,
                                               String uploadId) {
        return () -> {
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);

            try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
                file.seek((partNumber -1) * chunkSize);
                int bytesRead = file.getChannel().read(buffer);
                LOGGER.info("bytes read {} for part number {}", bytesRead, partNumber);
                buffer.flip(); // Swap position and limit before reading from the buffer.
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .contentLength((long) bytesRead)
                        .build();

                UploadPartResponse partResponse = s3Client.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromByteBuffer(buffer));

                return CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(partResponse.eTag())
                        .build();

            }
        };

    }
}
