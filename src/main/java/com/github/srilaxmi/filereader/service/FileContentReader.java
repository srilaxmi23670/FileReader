package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.dto.FileRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface FileContentReader {

    Mono<Void> initialize(String filePath, String sheetName);

    Flux<String> getHeaders();

    Flux<FileRow> getAllRows();

    Mono<FileRow> getNextRow(Integer rowNumber);

}
