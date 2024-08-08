package com.github.srilaxmi.filereader.controller;

import com.github.srilaxmi.filereader.service.FileDownloadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/api/v1/file-download")
public class FileDownloadController {

    private static final String FILE_NAME = "src/main/resources/Files/File.csv";

    @Autowired
    private FileDownloadService fileDownloadService;

    @PostMapping("/{field}/generate-file")
    public Mono<ResponseEntity<Resource>> generateAndDownloadFile() {

        return fileDownloadService.generateFile(FILE_NAME)
                .flatMap(fileCreated -> Mono.just(new FileSystemResource(FILE_NAME)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(resource -> {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentDispositionFormData(resource.getFilename(), resource.getFilename());
                    return Mono.just(
                            ResponseEntity
                                    .ok()
                                    .cacheControl(CacheControl.noCache())
                                    .headers(headers)
                                    .body(resource)
                    );
                });
    }

}
