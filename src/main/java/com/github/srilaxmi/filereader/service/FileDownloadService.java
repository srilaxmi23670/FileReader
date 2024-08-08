package com.github.srilaxmi.filereader.service;


import com.github.srilaxmi.filereader.util.FileGenerationUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.HEADERS;

@Service
@Slf4j
public class FileDownloadService {

    public Mono<Boolean> generateFile(String fileName) {

        return Flux.just(new Document())
                .collectList()
                .map(documents -> {

                    try {
                        FileGenerationUtil.writeToCSVFile(HEADERS, documents, fileName);
                    } catch (IOException e) {
                        log.error(e.getMessage());
                        return false;
                    }
                    return true;
                });
    }

}
