package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.constants.FileExtension;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

@Component
@Slf4j
public class FileContentReaderFactory {

    //Use this service through File Extraction service, not directly

    @Scope("prototype")
    public CSVFileReader getCsvFileReader() {
        return new CSVFileReader();
    }
    private static final long FILE_SIZE_THRESHOLD = 1024L * 1024L; // 1 MB threshold

    @Scope("prototype")
    public AsposeFileReader getAsposeFileReader() {
        return new AsposeFileReader();
    }

    @Scope("prototype")
    public ApacheFileReader getApacheFileReader() {
        return new ApacheFileReader();
    }

    public Mono<FileContentReader> createFileReaderService(
            String fileExtension, String filePath, String sheetName
    ) {

        FileExtension extension = FileExtension.valueOf(fileExtension);
        FileContentReader fileContentReader = initializeReaderMap(filePath).get(extension);

        return fileContentReader.initialize(filePath, sheetName).thenReturn(fileContentReader);
    }

    public Mono<FileContentReader> getFileReaderService(
            String filePath, String sheetName
    ) {

        String fileName = new File(filePath).getName();
        String fileExtension = FilenameUtils.getExtension(fileName).toUpperCase(Locale.ROOT);

        return createFileReaderService(fileExtension, filePath, sheetName);
    }

    private Map<FileExtension, FileContentReader> initializeReaderMap(String filePath) {

        EnumMap<FileExtension, FileContentReader> readerMap = new EnumMap<>(FileExtension.class);

        readerMap.put(FileExtension.CSV, this.getCsvFileReader());
        readerMap.put(FileExtension.TSV, this.getAsposeFileReader());
        readerMap.put(FileExtension.XLS, this.getAsposeFileReader());
        readerMap.put(FileExtension.XLSB, this.getAsposeFileReader());
        readerMap.put(FileExtension.XLSX, getFileReaderBasedOnLength(filePath));

        return readerMap;
    }

    private FileContentReader getFileReaderBasedOnLength(String filePath) {

        long fileLength = new File(filePath).length();
        return (fileLength >= FILE_SIZE_THRESHOLD) ? this.getAsposeFileReader() : this.getApacheFileReader();
    }


}
