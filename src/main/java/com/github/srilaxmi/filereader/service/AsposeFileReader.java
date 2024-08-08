package com.github.srilaxmi.filereader.service;

import com.aspose.cells.*;
import com.github.srilaxmi.filereader.dto.FileRow;
import com.github.srilaxmi.filereader.util.FileDataReaderUtil;
import com.github.srilaxmi.filereader.util.FileParseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.github.srilaxmi.filereader.util.FileParseUtil.getColumnsForXlsbAndXlsAndTsvFile;

@Component
@Scope("prototype")
@Slf4j
public class AsposeFileReader implements FileContentReader {

    private Workbook workbook;

    private  Iterator<Row> rowIterator;

    private List<String> headers;

    @Override
    public Mono<Void> initialize(String filePath, String sheetName) {

        try {

            log.info("Before generating workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            LoadOptions loadOptions = new LoadOptions();
            loadOptions.setMemorySetting(MemorySetting.MEMORY_PREFERENCE);

            this.workbook = new Workbook(filePath, loadOptions);

            log.info("After generating workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

             Worksheet worksheet = Objects.requireNonNullElse(workbook.getWorksheets().get(sheetName), workbook.getWorksheets().get(FileParseUtil.BASE_INDEX));

            log.info("After generating worksheet :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            this.rowIterator = worksheet.getCells().getRows().iterator();
            this.headers = extractHeaders();

            log.info("After generating headers and iterator :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            return Mono.empty();

        }  catch (Exception e) {
            log.error("ERROR while fetching worksheet: ", e);
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

        return Flux.fromIterable(() -> rowIterator)
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

        return getFileRow(rowIterator.next());
    }

    private List<String> extractHeaders() {

        Row columnContents = rowIterator.next();
        return getColumnsForXlsbAndXlsAndTsvFile(columnContents);
    }

    private Mono<FileRow> getFileRow(Row row) {

        if (!isRowEmpty(row)) {
            Document document = FileDataReaderUtil.generateDocumentForXlsbOrXlsOrTsvRow(row, headers);
            return Mono.just(FileRow.builder().row(document).build());
        } else {
            return Mono.empty();
        }

    }

    private Boolean isRowEmpty(Row row) {

        for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
            if (Objects.nonNull(row.getCellOrNull(colIndex)) &&
                    Objects.nonNull(row.getCellOrNull(colIndex).getValue()) &&
                    !StringUtils.isBlank(row.getCellOrNull(colIndex).getValue().toString().trim())) {
                return false; // Cell is not empty
            }
        }

        return true;
    }

    private void close() {

        if (Objects.nonNull(this.workbook)) {

            log.info("Before Disposing workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            this.workbook.dispose();

            log.info("After disposing workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

        }
    }

}
