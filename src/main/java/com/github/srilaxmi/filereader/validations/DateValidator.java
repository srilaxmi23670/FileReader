package com.github.srilaxmi.filereader.validations;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;

public class DateValidator implements Validator {
    private static DateTimeFormatter dateTimeFormatter;

    public DateValidator(DateTimeFormatter dateTimeFormatter) {
        DateValidator.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public boolean isValid(Object object, Boolean strictCheck) {

        try {
            if (strictCheck) {
                return Objects.nonNull(dateTimeFormatter) ?
                        Instant.from(dateTimeFormatter.parse(object.toString())) != null :
                        object instanceof Date;
            } else {
                isValueValidDate(object);
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private void isValueValidDate(Object value) {
        try {
            String valueString = Objects.toString(value, "");
            Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(valueString)));
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Failed to parse ISO_INSTANT string to Date: " + e.getMessage());
        }
    }

}
