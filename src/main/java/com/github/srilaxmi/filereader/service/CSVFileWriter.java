package com.github.srilaxmi.filereader.service;

import com.opencsv.CSVWriter;
import org.bson.Document;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CSVFileWriter implements FileContentWriter {

    private CSVWriter csvWriter;
    private List<String> headers;

    @Override
    public void initialize(String filePath) throws IOException {
        FileWriter fileWriter = new FileWriter(filePath);
        this.csvWriter = new CSVWriter(fileWriter);
    }

    @Override
    public void writeHeader(List<String> headers) {
        csvWriter.writeNext(headers.toArray(new String[0]));
        this.headers = headers;
    }

    @Override
    public void writeRows(List<Document> rows) {
        List<String[]> rowsToWrite = rows
                .stream()
                .map(this::convertDocumentToRow)
                .collect(Collectors.toList());
        csvWriter.writeAll(rowsToWrite);
    }

    public String[] convertDocumentToRow(Document document) {

        String[] row = new String[this.headers.size()];
        for (int i = 0; i < this.headers.size(); i++) {
            String header = this.headers.get(i);
            if (document.containsKey(header) && Objects.nonNull(document.get(header))) {
                row[i] = document.get(header).toString();
            } else {
                row[i] = "";
            }
        }
        return row;
    }

    public void finish() throws IOException {
        this.csvWriter.close();
    }
}
