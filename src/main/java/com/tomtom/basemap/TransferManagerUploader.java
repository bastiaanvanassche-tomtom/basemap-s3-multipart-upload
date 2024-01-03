package com.tomtom.basemap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

public class TransferManagerUploader implements Uploader{
    @Override
    public void multipartUploadWithS3Client(final String filePath, final String bucketName, final String key, final String region, final long chunkSize) {
        final S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.of(region))
                .targetThroughputInGbps(20.0)
                .minimumPartSizeInBytes(chunkSize)
                .build();

        final S3TransferManager transferManager =
                S3TransferManager.builder()
                        .s3Client(s3AsyncClient)
                        .build();



        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucketName).key(key))
                .addTransferListener(LoggingTransferListener.create())
                .source(Paths.get(filePath))
                .build();

        FileUpload upload = transferManager.uploadFile(uploadFileRequest);
        upload.completionFuture().join();
    }
}
