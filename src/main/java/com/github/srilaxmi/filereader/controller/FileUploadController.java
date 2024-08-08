package com.github.srilaxmi.filereader.controller;

import com.github.srilaxmi.filereader.service.BatchOperationService;
import com.github.srilaxmi.filereader.util.FileParseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequestMapping("/api/v1/file-upload")
public class FileUploadController {

    @Autowired
    private BatchOperationService batchOperationService;

    @Value("${fileuploadpath}")
    private String fileUploadPath;

    @PostMapping("/preprocess/sheet-names")
    public Mono<List<String>> uploadFileForPreProcessing(
            @RequestPart Mono<FilePart> file
    ) {

        final Path basePath = Paths.get(fileUploadPath);
        return file
                .flatMap(fp -> {
                    String fileName = fp.filename();
                    Path path = basePath.resolve(fileName);
                    return fp.transferTo(path).thenReturn(path);
                })
                .flatMap(path -> {
                    try {
                        return Mono.just(FileParseUtil.getSheetNamesInXlsxOrXlsbOrXlsFile(path.toString()));
                    } catch (Exception e) {
                        return Mono.error(new Exception(e.getMessage()));
                    }
                }
        );
    }

    @PostMapping("/{collection}")
    public Mono<Boolean> uploadFileRequiredForMigrations(
            @PathVariable String collection,
            @RequestPart Mono<FilePart> file,
            @RequestPart(required = false) String sheetName
    ) {

        final Path basePath = Paths.get(fileUploadPath);

        return file
                .flatMap(fp -> {
                    String fileName = fp.filename();
                    Path path = basePath.resolve(fileName);
                    return fp.transferTo(path).thenReturn(path);
                })
                .flatMap(path -> {
                    try {
                        batchOperationService.saveFileDataToCollection(path.toString(), sheetName, collection).subscribe();
                        return Mono.just(Boolean.TRUE);
                    } catch (IOException e) {
                        return Mono.error(e);
                    }
                });
    }


}
