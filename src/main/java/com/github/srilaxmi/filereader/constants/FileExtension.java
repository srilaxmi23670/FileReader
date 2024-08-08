package com.github.srilaxmi.filereader.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum FileExtension {

    CSV("CSV"),
    TSV("TSV"),
    XLS("XLS"),
    XLSB("XLSB"),
    XLSX("XLSX");

    private final String label;

}
