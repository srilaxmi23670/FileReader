package com.github.srilaxmi.filereader.util;

import com.github.srilaxmi.filereader.constants.DataType;
import com.github.srilaxmi.filereader.constants.GlobalConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.bson.Document;

import java.util.Date;
import java.util.Objects;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.removeCommaTrailingNonBreakingSpaces;

@Slf4j
public class DataTypeUtil {

    public static DataType getDefaultDataType() {

        return DataType.STRING;
    }

    public static DataType findDataTypeForValue(String value) {

        if (StringUtils.isBlank(value)) {
            return getDefaultDataType();
        }

        for (DataType dataType : DataType.values()) {

            if (!Objects.equals(dataType, DataType.GEOCODE)) {

                if (dataType.isValid(value, null, Boolean.FALSE))  {
                    return dataType;
                }

            }
        }

        return getDefaultDataType();
    }

    public static boolean isValueStringType(Object value, Boolean strictCheck) {

        if (strictCheck) {
            return value instanceof String || Objects.isNull(value);
        } else {
            return true;
        }

    }

    public static Boolean isValueNumberType(Object value, Boolean strictCheck) {

        return isValueDoubleType(value, strictCheck) || isValueFloatType(value, strictCheck) || isValueIntegerType(value, strictCheck);
    }

    public static Boolean isValueIntegerType(Object rawValue, Boolean strictCheck) {

        try {

            if (strictCheck) {
                return rawValue instanceof Integer;
            } else {
                String value = removeCommaTrailingNonBreakingSpaces(rawValue.toString());
                Integer.parseInt(value);
                return Boolean.TRUE;
            }

        } catch (IllegalArgumentException | NullPointerException e) {
            return Boolean.FALSE;
        }
    }

    public static Boolean isValueDoubleType(Object rawValue, Boolean strictCheck) {

        try {

            if (strictCheck) {
                return rawValue instanceof Double;
            } else {
                String value = removeCommaTrailingNonBreakingSpaces(rawValue.toString());
                Double.parseDouble(value);
                return Boolean.TRUE;
            }

        } catch (NumberFormatException | NullPointerException e) {
            return Boolean.FALSE;
        }
    }

    public static Boolean isValueFloatType(Object rawValue, Boolean strictCheck) {

        try {

            if (strictCheck) {
                return rawValue instanceof Float;
            } else {
                String value = removeCommaTrailingNonBreakingSpaces(rawValue.toString());
                Float.parseFloat(value);
                return Boolean.TRUE;
            }

        } catch (NumberFormatException | NullPointerException e) {
            return Boolean.FALSE;
        }
    }

    public static Document convertFieldToDateInDocumentIfPresent(
            Document document,
            String field
    ) {

        if (Objects.nonNull(document.getOrDefault(field, null))) {
            document.put(field, DataType.DATE.convert(document.get(field)));
        }

        return document;
    }

    public static Document convertDateFieldToISOStringIfPresent(
            Document document,
            String field
    ) {

        if (
                Objects.nonNull(document.getOrDefault(field, null)) && document.get(field) instanceof Date
        ) {

            document.put(
                    field,
                    DateFormatUtils.format(
                            (Date) document.get(field),
                            GlobalConstants.ISO_DATE_FORMAT
                    )
            );
        }

        return document;
    }

}
