package com.tomtom.basemap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class UploaderTest {
    @Test
    @Disabled
    void custom() {
        new CustomUploader()
                .multipartUploadWithS3Client("src/test/resources/cyprus-latest.osm.pbf",
                        "omf-internal-usw2",
                        "transportation/cyprus-latest.osm.pbf",
                        "us-west-2",
                        6*1024*1024);
    }
    @Test
    @Disabled
    void tf() {
        new TransferManagerUploader()
                .multipartUploadWithS3Client("src/test/resources/cyprus-latest.osm.pbf",
                        "omf-internal-usw2",
                        "transportation/cyprus-latest.osm.pbf",
                        "us-west-2",
                        6*1024*1024);
    }

}