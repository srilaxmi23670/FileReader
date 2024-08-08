package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.constants.FileExtension;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Component
@Slf4j
public class FileExtractionService {

    @Autowired
    private FileContentReaderFactory fileContentReaderFactory;

    public Flux<FileContentReader> extractFile(
            String filePath, String sheetName
    ) throws IOException {

        String fileName = new File(filePath).getName();
        String fileExtension = FilenameUtils.getExtension(fileName).toUpperCase(Locale.ROOT);

        if (StringUtils.equalsIgnoreCase(fileExtension, "ZIP")) {
            return extractZipFile(filePath, sheetName);
        } else {
            return fileContentReaderFactory.createFileReaderService(fileExtension, filePath, sheetName)
                    .flatMapMany(Flux::just);
        }

    }

    private Flux<FileContentReader> extractZipFile(
            String filePath, String sheetName
    ) throws IOException {

        log.info("Extracting zip file :: {}", filePath);

        Path extractionDir = Files.createTempDirectory("");

        return Mono.fromCallable(() -> new ZipFile(filePath))
                .flatMapMany(zipFile -> getZipEntryFlux(filePath, zipFile)
                        .concatMap(zipEntry -> {
                            try {
                                return getFileContentReaderMono(sheetName, zipEntry, extractionDir, zipFile);
                            } catch (IOException e) {
                                log.error("Error :: ", e);
                                return Flux.error(e);
                            }
                        })
                        .doFinally(signalType -> {
                            if (signalType == SignalType.ON_ERROR) {
                                log.error("doOnFinally: Stream terminated with an error");
                            } else {
                                log.info("doOnFinally: Stream terminated normally");
                            }
                            clearGarbageResources(zipFile);
                        }));
    }

    private Mono<FileContentReader> getFileContentReaderMono(
            String sheetName, ZipEntry zipEntry, Path extractionDir, ZipFile zipFile
    ) throws IOException {

        String entryName = zipEntry.getName();
        String fileName = entryName.contains("/") ? entryName.substring(entryName.lastIndexOf('/') + 1) : entryName;
        Path outputPath = extractionDir.resolve(fileName);

        log.info("{} :: check if extension is valid for outputPath :: {}", zipEntry.getName(), outputPath);
        Boolean isFileExtensionValid = checkIfFileExtensionApplicable(outputPath);

        if (!isFileExtensionValid) {
            log.info("{} :: file extension is not valid for file name :: {}", zipEntry.getName(), outputPath);
            return Mono.empty();
        }

        log.info("{} :: Copying zip file entry :: to filePath :: {} Total Memory :: {}, Free Memory :: {}", zipEntry.getName(), outputPath, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

        try (InputStream inputStream = zipFile.getInputStream(zipEntry)) {

            Files.copy(inputStream, outputPath, StandardCopyOption.REPLACE_EXISTING);

            log.info("{} :: Parsing zip file entry :: Total Memory :: {}, Free Memory :: {}", outputPath, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
            return fileContentReaderFactory.getFileReaderService(String.valueOf(outputPath), sheetName);
        }
    }

    private Boolean checkIfFileExtensionApplicable(Path outputPath) {

        String fileNameString = new File(String.valueOf(outputPath)).getName();
        String fileExtension = FilenameUtils.getExtension(fileNameString).toUpperCase(Locale.ROOT);
        List<String> applicableFileExtensions = Stream.of(FileExtension.values()).map(Objects::toString).collect(Collectors.toList());

        return applicableFileExtensions.contains(fileExtension);
    }

    private Flux<ZipEntry> getZipEntryFlux(String filePath, ZipFile zf) {

        List<ZipEntry> zipEntries = new ArrayList<>();
        Enumeration<? extends ZipEntry> entries = zf.entries();

        while (entries.hasMoreElements()) {
            ZipEntry zipEntry = entries.nextElement();
            if (!zipEntry.isDirectory()) {
                zipEntries.add(zipEntry);
            }
        }

        log.info("Iterating over :: {} zip file entries :: {}", zipEntries.size(), filePath);
        return Flux.fromIterable(zipEntries);
    }

    private void clearGarbageResources(ZipFile zipFile) {
        log.info("{} :: Before closing zip file :: Total Memory :: {}, Free Memory :: {}", zipFile, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

        try {
            zipFile.close();
            log.info("Closed zip file: {}", zipFile.getName());
        } catch (IOException e) {
            log.warn("Error closing zip file: {}", zipFile.getName(), e);
        }

        log.info("{} :: After closing zip file :: Total Memory :: {}, Free Memory :: {}", zipFile, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

    }

}
