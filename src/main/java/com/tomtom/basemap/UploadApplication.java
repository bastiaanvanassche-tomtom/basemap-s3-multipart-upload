package com.tomtom.basemap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(name = "s3-multipart-upload", mixinStandardHelpOptions = true, version = "1.0",
        description = "Uploads a file to AWS S3 using multipart upload")
public class UploadApplication implements Callable<Integer> {
    private final Logger LOGGER = LoggerFactory.getLogger(UploadApplication.class);

    @Parameters(index = "0", description = "The file to upload.")
    private File file;

    @Option(names = {"--bucket"}, description = "", required = true)
    private String bucket;

    @Option(names = {"--key"}, description = "", required = true)
    private String key;

    @Option(names = {"--region"}, description = "", required = true)
    private String region;
    @Option(names = {"--chunk-size"}, description = "")
    private long chunkSize = 6 * 1024 * 1024;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new UploadApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            Instant start = Instant.now();

            new TransferManagerUploader()
                    .multipartUploadWithS3Client(file.getPath(),
                            bucket,
                            key,
                            region,
                            chunkSize);
            Instant finish = Instant.now();
            Duration timeElapsed = Duration.between(start, finish);
            LOGGER.info("Upload took {} ", toTimeInHHMMSS(timeElapsed));

            return 0;

        } catch (Exception e) {
            LOGGER.error("Unexpected error while attempting to execute upload", e);
            return 1;
        }
    }
    private String toTimeInHHMMSS(Duration duration){
        long HH = duration.toHours();
        long MM = duration.toMinutesPart();
        long SS = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", HH, MM, SS);
    }
}
