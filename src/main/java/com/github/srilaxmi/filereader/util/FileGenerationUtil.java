package com.github.srilaxmi.filereader.util;

import com.github.srilaxmi.filereader.service.CSVFileWriter;
import com.opencsv.CSVWriter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@UtilityClass
public class FileGenerationUtil {

    @Value("${fileuploadpath}")
    private String fileUploadPath;

    public static String getFilepath(String fileName) {
        return fileUploadPath + fileName;
    }

    public void createFileIfNotExists(String fileName) {

        File file = new File(fileName);
        createFileIfNotExists(fileName, file, fileName);
    }

    public void createFileIfNotExists(String fileName, File file, String filePath) {

        try {

            if (!file.exists()) {
                if (file.getParentFile() != null && file.getParentFile().mkdirs()) {
                    log.info("Folder Created with name :: {}", file.getParentFile().getAbsolutePath());
                }
                if (file.createNewFile()) {
                    log.info("File Created with name :: {}", fileName);
                }

            } else {
                log.info("File already exists with name :: {}", fileName);
            }

        } catch (IOException e) {
            log.error("An error occurred while creating the file: {}", filePath, e);
        }
    }

    public void writeToCSVFile(String[] headers, List<Document> documents, String fileName) throws IOException {

        List<String[]> csvData = generateCSVData(headers, documents);
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(fileName))) {
            csvWriter.writeAll(csvData);
        }
    }

    public List<String[]> generateCSVData(String[] headers, List<Document> documents) {

        List<String[]> list = new ArrayList<>();
        list.add(headers);

        for (Document document : documents) {

            String[] recordData = new String[headers.length];

            for (int i = 0; i < headers.length; i++) {
                Object value = document.getOrDefault(headers[i], "");
                recordData[i] = Objects.isNull(value) || value == "" ? "null" : value.toString();
            }
            list.add(recordData);
        }
        return list;
    }

}
