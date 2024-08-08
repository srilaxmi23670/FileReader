package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.dto.FileRow;
import com.github.srilaxmi.filereader.util.FileParseUtil;
import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.Document;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.github.srilaxmi.filereader.util.FileParseUtil.replaceNewlinesAndStrip;

@Component
@Scope("prototype")
@Slf4j
public class ApacheFileReader implements FileContentReader {

    private XSSFWorkbook workbook;
    private Iterator<Row> rowIterator;
    private List<String> headers;

    public static Document generateDocumentForXlsxRow(
            XSSFRow rowContents, List<String> columns
    ) {

        Document rowData = new Document();
        DataFormatter formatter = new DataFormatter();

        for (int j = 0; j < columns.size(); j++) {
            try {
                Cell cell = rowContents.getCell(j);
                String column = columns.get(j);

                if (StringUtils.isNotBlank(column) && cell != null) {
                    String cellValue = formatter.formatCellValue(cell);
                    rowData.put(column, cellValue.strip());
                } else {
                    rowData.put(column, "");
                }
            } catch (NullPointerException e) {
                rowData.put(columns.get(j), "");
            }
        }

        return rowData;
    }

    @Override
    public Mono<Void> initialize(String filePath, String sheetName) {

        try (FileInputStream fis = new FileInputStream(new File(filePath))) {

            log.info("Before generating workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            this.workbook = new XSSFWorkbook(fis);

            log.info("After generating workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            XSSFSheet worksheet = Objects.nonNull(sheetName) ? workbook.getSheet(sheetName) : workbook.getSheetAt(FileParseUtil.BASE_INDEX);

            log.info("After generating worksheet :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            this.rowIterator = worksheet.iterator();
            this.headers = extractHeaders();

            log.info("After generating headers :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

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
                .flatMap(row -> getFileRow((XSSFRow) row))
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

        return getFileRow((XSSFRow) rowIterator.next());
    }

    private List<String> extractHeaders() {
        XSSFRow columnContents = (XSSFRow) rowIterator.next();
        return getColumnsForXlsxFile(columnContents);
    }

    private List<String> getColumnsForXlsxFile(XSSFRow row) {
        List<String> contentColumns = new ArrayList<>();

        for (Cell cell : row) {
            if (Objects.nonNull(cell)) {
                contentColumns.add(replaceNewlinesAndStrip(cell.getStringCellValue()));
            }
        }

        return contentColumns;
    }

    private Mono<FileRow> getFileRow(XSSFRow row) {

        if (!isRowEmpty(row)) {
            Document document = generateDocumentForXlsxRow(row, headers);
            return Mono.just(FileRow.builder().row(document).build());
        } else {
            return Mono.empty();
        }

    }

    private Boolean isRowEmpty(XSSFRow row) {

        if (row == null) {
            return true;
        }

        for (int i = row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
            Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }

        return true;
    }

    private void close() {

        if (Objects.nonNull(this.workbook)) {
            try {

                log.info("Before Disposing workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

                this.workbook.close();

                log.info("After disposing workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

            } catch (IOException e) {
                log.warn("Error while Disposing workbook :: Total Memory :: {}, Free Memory :: {}", (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
            }
        }
    }

}
