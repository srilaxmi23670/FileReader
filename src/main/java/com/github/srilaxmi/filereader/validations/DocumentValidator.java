package com.github.srilaxmi.filereader.validations;

import org.bson.Document;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.convertToDocument;

public class DocumentValidator implements Validator {
    @Override
    public boolean isValid(Object input, Boolean strictCheck) {
        try {
            if (strictCheck) {
                return input instanceof Document;
            } else {
                Object value = convertToDocument(input);
                return true;
            }
        } catch (RuntimeException e) {
            return false;
        }
    }
}
