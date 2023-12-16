package com.tomtom.basemap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UploaderTest {
    @Test
    @Disabled
    void name() {
        new Uploader()
                .multipartUploadWithS3Client("src/test/resources/cyprus-latest.osm.pbf",
                        "omf-internal-usw2",
                        "transportation/cyprus-latest.osm.pbf",
                        "us-west-2",
                        6*1024*1024);
    }
}