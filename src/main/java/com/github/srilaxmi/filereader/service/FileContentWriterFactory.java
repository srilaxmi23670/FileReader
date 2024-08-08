package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.constants.FileExtension;
import org.apache.commons.io.FilenameUtils;

import java.util.Locale;

public class FileContentWriterFactory {

    public FileContentWriter getWriter(String filePath) {
        String fileExtension = FilenameUtils.getExtension(filePath).toUpperCase(Locale.ROOT);
        switch (FileExtension.valueOf(fileExtension)) {
            case CSV:
                return new CSVFileWriter();
            default:
                throw new RuntimeException("File format not found for extension : " + fileExtension);
        }
    }
}
