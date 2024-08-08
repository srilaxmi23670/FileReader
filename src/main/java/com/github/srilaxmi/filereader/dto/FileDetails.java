package com.github.srilaxmi.filereader.dto;

import lombok.Builder;
import lombok.Data;

import java.io.File;

@Data
@Builder
public class FileDetails {

    private String destinationUrl;
    private String fileId;
    private File file;
}
