package com.github.srilaxmi.filereader.util;

import com.aspose.cells.Cell;
import com.aspose.cells.Row;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileDataReaderUtil {

    public static Document generateDocumentForCsvRow(
            List<String> record, List<String> columns
    ) {

        Document rowData = new Document();

        for (int i = 0; i < record.size(); i++) {

            try {

                String column = columns.get(i);
                if (!StringUtils.isBlank(column)) {
                    rowData.put(column, record.get(i).strip());
                }

            } catch (NullPointerException np) {
                rowData.put(columns.get(i), "");
            }
        }

        return rowData;
    }

    public static Document generateDocumentForXlsbOrXlsOrTsvRow(
            Row rowContents, List<String> columns
    ) {

        Document rowData = new Document();
        for (int j = 0; j < columns.size(); j++) {

            try {

                Cell cell = rowContents.get(j);
                String column = columns.get(j);
                if (!StringUtils.isBlank(column)) {
                    rowData.put(column, cell.getStringValue().strip());
                }

            } catch (NullPointerException np) {
                rowData.put(columns.get(j), "");
            }
        }

        return rowData;
    }

    public static FileSystemResource createZipFile(List<File> files, String zipFileName) {
        try {
            File zipFile = File.createTempFile(zipFileName, ".zip");
            try (FileOutputStream fos = new FileOutputStream(zipFile);
                 ZipOutputStream zos = new ZipOutputStream(fos)) {
                for (File file : files) {
                    addToZipFile(file, zos);
                }
            }
            return new FileSystemResource(zipFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create zip file", e);
        }
    }

    private static void addToZipFile(File file, ZipOutputStream zos) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            ZipEntry zipEntry = new ZipEntry(file.getName());
            zos.putNextEntry(zipEntry);
            byte[] bytes = new byte[1024];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zos.write(bytes, 0, length);
            }
        }
    }
}
