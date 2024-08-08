package com.github.srilaxmi.filereader.dto;

import com.github.srilaxmi.filereader.constants.UploadStatus;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class FileUploadStatus {

    private String fileDestinationUrl;
    private String fileId;
    private UploadStatus uploadStatus;
}
