package com.github.srilaxmi.filereader.service;

import org.bson.Document;

import java.io.IOException;
import java.util.List;

public interface FileContentWriter {
    void initialize(String filePath) throws IOException;
    void writeHeader(List<String> headers);
    void writeRows(List<Document> rows);
    void finish() throws IOException;
}
