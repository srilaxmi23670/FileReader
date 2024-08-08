package com.github.srilaxmi.filereader.util;

import com.aspose.cells.Cell;
import com.aspose.cells.Row;
import com.aspose.cells.Workbook;
import com.aspose.cells.Worksheet;
import com.github.srilaxmi.filereader.constants.DataType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.getConvertedValue;
@Slf4j
public class FileParseUtil {

    public static final Integer BASE_INDEX = 0;

    private static final String SPECIAL_CHARS_REGEX = "&nbsp;|[\n /:?#\\[\\]@!$&'*+,;=]+";

    public static Map<String, DataType> getDataTypeMap() {

        Map<String, DataType> dataTypeMap = new HashMap();
        return dataTypeMap;
    }

    public static Document createMapForSingleRecordForCsv(
            List<String> record, List<String> columns, Map<String, DataType> dataTypeMap
    ) {

        Document rowData = new Document();

        for (int i = 0; i < record.size(); i++) {
            String value = record.get(i);
            if (value != null) {
                value = value.strip();
            }
            String column = columns.get(i);
            try {
                rowData.put(column, getConvertedValue(column, value, dataTypeMap));
            } catch (NullPointerException npe) {
                rowData.put(column, "");
            }

        }

        return rowData;
    }

    public static Document createMapForSingleRecordForXlsbOrXlsOrTsvFile(
            Row rowContents, List<String> columns, Map<String, DataType> dataTypeMap
    ) {

        Document rowData = new Document();
        for (int j = 0; j < columns.size(); j++) {

            try {

                String value = rowContents.get(j).getStringValue().strip();
                rowData.put(columns.get(j), getConvertedValue(columns.get(j), value, dataTypeMap));

            } catch (NullPointerException np) {
                rowData.put(columns.get(j), "");
            }
        }

        return rowData;
    }

    public static List<String> getColumnsForXlsbAndXlsAndTsvFile(Row columns) {

        Iterator<Cell> columnIterator = columns.iterator();
        List<String> contentColumns = new ArrayList<>();

        int cellNumber = 0;

        while(columnIterator.hasNext()) {
            contentColumns.add(replaceNewlinesAndStrip(columns.get(cellNumber).getStringValue()));
            cellNumber++;
        }

        return contentColumns;
    }

    public static Boolean isEmptyRowForCSV(String[] row) {

        for (String value : row) {
            if (value != null && !value.trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public static Boolean isXlsxOrXlsbOrXlsFile(String path) {
        String[] split = new File(path).getName().toUpperCase(Locale.ROOT).split("\\.");
        String extension = split[split.length-1];
        return extension.equals("XLSX") || extension.equals("XLS") || extension.equals("XLSB");
    }

    public static List<String> getSheetNamesInXlsxOrXlsbOrXlsFile(String path) throws Exception {

        if (isXlsxOrXlsbOrXlsFile(path)) {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(path));
            List<String> sheetNames = new ArrayList<>();
            Workbook workbook = new Workbook(bufferedInputStream);
            for (int i = 0; i < workbook.getWorksheets().getCount(); i++) {
                Worksheet sheet = workbook.getWorksheets().get(i);
                sheetNames.add(sheet.getName());
            }
            bufferedInputStream.close();
            return sheetNames;
        }
        return new ArrayList<>();
    }

    public static String replaceNewlinesAndStrip(String value) {
        value = value.replaceAll(SPECIAL_CHARS_REGEX, " ");
        return value.strip();
    }

}
