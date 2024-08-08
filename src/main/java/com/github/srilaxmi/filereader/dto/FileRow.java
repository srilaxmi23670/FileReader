package com.github.srilaxmi.filereader.dto;

import lombok.Builder;
import lombok.Data;
import org.bson.Document;

@Builder
@Data
public class FileRow {

    private Document row;

}
