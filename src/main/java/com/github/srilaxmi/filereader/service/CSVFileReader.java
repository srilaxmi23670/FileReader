package com.github.srilaxmi.filereader.service;

import com.opencsv.CSVReader;
import com.github.srilaxmi.filereader.dto.FileRow;
import com.github.srilaxmi.filereader.util.FileParseUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.srilaxmi.filereader.util.FileDataReaderUtil.generateDocumentForCsvRow;

@Component
@Scope("prototype")
@Slf4j
public class CSVFileReader implements FileContentReader {

    private CSVReader csvReader;
    private List<String> headers;

    @Override
    public Mono<Void> initialize(String filePath, String sheetName) {

        try {

            log.info("Generating CSV Reader :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            this.csvReader =  new CSVReader(new FileReader(filePath));
            this.headers = extractHeaders(csvReader);
            return Mono.empty();

        } catch(IOException e) {
            log.error("ERROR while generating reader: ", e);
            close();
            return Mono.error(e);
        }

    }

    @Override
    public Flux<String> getHeaders() {

        return Flux.fromIterable(headers);
    }

    @Override
    public Flux<FileRow> getAllRows() {

        Iterator<String[]> csvIterator = csvReader.iterator();
        return Flux.fromIterable(() -> csvIterator)
                .flatMap(this::getFileRow)
                .doFinally(signalType -> {
                    if (signalType == SignalType.ON_ERROR) {
                        log.error("doOnFinally: Stream terminated with an error");
                    } else {
                        log.info("doOnFinally: Stream terminated normally");
                    }
                    close();
                });
    }

    @Override
    public Mono<FileRow> getNextRow(Integer rowNumber) {

        try  {
            return Mono.just(csvReader.readNext())
                    .flatMap(this::getFileRow);
        } catch(IOException e) {
            return Mono.error(e);
        }
    }

    private Mono<FileRow> getFileRow(String[] values) {

        if (!isRowEmpty(values)) {
            Document document = generateDocumentForCsvRow(List.of(values), headers);
            return Mono.just(FileRow.builder().row(document).build());
        } else {
            return Mono.empty();
        }
    }

    private Boolean isRowEmpty(String[] row) {

        for (String value : row) {
            if (value != null && !value.trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private List<String> extractHeaders(CSVReader csvReader) throws IOException {

        return Stream.of(csvReader.readNext())
                .map(FileParseUtil::replaceNewlinesAndStrip)
                .collect(Collectors.toList());
    }

    private void close() {

        if (Objects.nonNull(this.csvReader)) {
            try {
                log.info("Before Closing CSVReader :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
                this.csvReader.close();
                log.info("After Closing CSVReader :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            } catch (IOException e) {
                log.warn("ERROR while closing csv-reader");
            }
        }

    }

}
