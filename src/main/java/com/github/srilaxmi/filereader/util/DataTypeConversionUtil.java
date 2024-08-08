package com.github.srilaxmi.filereader.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.srilaxmi.filereader.constants.DataType;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.json.JsonObject;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.TRAILING_NON_BREAKING_SPACES_COMMA_REMOVER_REGEX;
import static com.github.srilaxmi.filereader.util.DataTypeUtil.isValueDoubleType;
import static com.github.srilaxmi.filereader.util.DataTypeUtil.isValueNumberType;


@UtilityClass
@Slf4j
public class DataTypeConversionUtil {

    public final static ObjectMapper objectMapper = new ObjectMapper();

    public static Object getConvertedValue(String field, Object value, Map<String, DataType> fieldDataTypeMap) {

        if (!CollectionUtils.isEmpty(fieldDataTypeMap) && fieldDataTypeMap.containsKey(field)) {
            DataType dataType = fieldDataTypeMap.get(field);
            return getConvertedOrDefault(field, value, dataType);
        } else {
            return value;
        }

    }

    public static Object getConvertedOrDefault(String fieldName, Object value, DataType dataType) {

        try {

            return ObjectUtils.isEmpty(value) ? value : dataType.convert(value);

        } catch (RuntimeException e) {
            log.error(String.format(":: ERROR : For Field: '%s' with value '%s' required dataType '%s'", fieldName, value, dataType));
            return value;
        }

    }

    public static Object convertToNumber(Object rawValue) {

        if (ObjectUtils.isEmpty(rawValue)) {
            return "";
        }

        try {
            return convertToNumber(rawValue, Integer.class, 0, Boolean.FALSE);
        } catch (NumberFormatException e) {
            return convertToNumber(rawValue, Double.class, 0.0, Boolean.FALSE);
        }
    }

    public static Boolean convertToBoolean(Object value) {
        return Objects.nonNull(value) && Boolean.parseBoolean(value.toString());
    }

    public static Date convertToDate(Object value) {
        try {
            String valueString = Objects.toString(value, "");
            return Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(valueString)));
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Failed to parse ISO_INSTANT string to Date: " + e.getMessage());
        }
    }

    public static Date convertToSimpleFormatDate(Object value) throws ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.parse(value.toString());
    }

    public static Object convertToNumberOrDateOrGetAsIs(Object value) {
        try {
            value = DataType.NUMBER.convert(value);
        } catch (NumberFormatException e) {
            log.warn("Couldn't convert {} to Number", value);
            try {
                value = DataTypeConversionUtil.convertToSimpleFormatDate(value);
            } catch (ParseException ex) {
                log.warn("Couldn't convert {} to Date", value);
            }
        }
        return value;
    }

    public static Document convertToDocument(Object valueObj) {

        if (valueObj instanceof Document) {     //when imported file through mongoDBCompass, saved as document
            return (Document) valueObj;
        } else {
            String valueString = valueObj.toString();

            try {
                return objectMapper.readValue(valueString, Document.class);
            } catch (JsonProcessingException e) {
                log.error("Exception during object conversion to document :: {} :: ", valueString, e);
            }

            Document doc;
            try {
                log.info("Converting to JSON by replacing dots in string :: {} :: ", valueString);
                if (!isValueNumberType(valueString, Boolean.FALSE) && valueString.contains(".")) {
                    valueString = valueString.replace(".", "..");
                }

                doc = objectMapper.readValue(valueString, Document.class);
                //As keys and values gets changed during above replace
                doc = replaceSearchStringsInKeys(doc, "..", "_");
                doc = replaceSearchStringInValues(doc, "..", ".");
                return doc;
            } catch (Exception e) {
                log.error("Exception during object conversion to document :: {} :: ", valueString, e);
                throw new RuntimeException(e);
            }
        }
    }

    public static List<String> convertToStringArray(Object value) {

        String input = Objects.toString(value, "");
        String[] values;

        if (isEnclosedInBrackets(input)) {
            values = input.substring(1, input.length() - 1).split(",");
        } else if (input.contains(",")) {
            values = input.split(",");
        } else {
            values = new String[]{input.trim()};
        }

        return Arrays.stream(values)
                .map(String::trim)
                .filter(listValue -> !ObjectUtils.isEmpty(listValue))
                .collect(Collectors.toList());
    }

    public static List<Object> strictConvertToStringArray(Object value) {

        String input = Objects.toString(value, "");

        if (isEnclosedInBrackets(input)) {
            String[] values = input.substring(1, input.length() - 1).split(",");
            return Arrays.stream(values)
                    .map(String::trim)
                    .filter(listValue -> !ObjectUtils.isEmpty(listValue))
                    .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("Unsupported input format for arrayConversion");
        }

    }

    public static List<Object> convertToNumberArray(Object value) {

        return convertToStringArray(value)
                .stream()
                .filter(listValue -> !ObjectUtils.isEmpty(listValue))
                .map(DataTypeConversionUtil::convertToNumber)
                .collect(Collectors.toList());
    }

    public static List<Object> strictConvertToNumberArray(Object value) {

        return strictConvertToStringArray(value)
                .stream()
                .filter(listValue -> !ObjectUtils.isEmpty(listValue))
                .map(DataTypeConversionUtil::convertToNumber)
                .collect(Collectors.toList());
    }


    public static Double convertToDouble(Object rawValue, Boolean fetchOneDecimal) {
        return convertToNumber(rawValue, Double.class, 0.0, Boolean.FALSE, fetchOneDecimal);
    }

    public static Float convertToFloat(Object rawValue, Boolean fetchOneDecimal) {
        return convertToNumber(rawValue, Float.class, 0.0F, Boolean.FALSE, fetchOneDecimal);
    }

    public static Integer convertToInteger(Object rawValue) {
        return convertToNumber(rawValue, Integer.class, 0, Boolean.TRUE);
    }

    public static <T extends Number> T convertToNumber(Object rawValue, Class<T> targetType, T defaultValue, Boolean forceIntegerParse) {

        return convertToNumber(rawValue, targetType, defaultValue, forceIntegerParse, Boolean.FALSE);

    }

    public static <T extends Number> T convertToNumber(Object rawValue, Class<T> targetType, T defaultValue, Boolean forceIntegerParse, Boolean fetchOneDecimal) {

        if (ObjectUtils.isEmpty(rawValue)) {
            return defaultValue;
        }

        String rawValueString = rawValue.toString().replaceAll(TRAILING_NON_BREAKING_SPACES_COMMA_REMOVER_REGEX, "");
        String value = removeTrailingZeroesIfDataTypeIsNumber(rawValueString, fetchOneDecimal);

        try {

            if (Objects.equals(targetType, Double.class)) {
                return targetType.cast(Double.valueOf(value));
            } else if (Objects.equals(targetType, Float.class)) {
                return targetType.cast(Float.valueOf(value));
            } else if (Objects.equals(targetType, Integer.class) && forceIntegerParse) {
                return targetType.cast(Double.valueOf(value).intValue());
            } else if (Objects.equals(targetType, Integer.class) && !forceIntegerParse) {
                return targetType.cast(Integer.valueOf(value));
            }

        } catch (NumberFormatException e) {
            throw new NumberFormatException(e.getMessage());
        }

        return defaultValue;
    }

    public static String removeTrailingZeroesIfDataTypeIsNumber(String rawValue, Boolean fetchOneDecimal) {

        if (StringUtils.isEmpty(rawValue) || !isValueDoubleType(rawValue, Boolean.FALSE)) {
            return Objects.requireNonNullElse(rawValue, "");
        }

        String value = removeCommaTrailingNonBreakingSpaces(rawValue);

        Double doubleValue = Double.valueOf(value);
        BigDecimal decimalValue = BigDecimal.valueOf(doubleValue);
        int scale = decimalValue.stripTrailingZeros().scale();

        if (scale <= 0) {
            return String.valueOf(doubleValue.intValue()); // Return as integer if no decimal places
        } else if (fetchOneDecimal) {
            DecimalFormat format = new DecimalFormat("0.#"); // Return value with one decimal place
            return format.format(doubleValue);
        } else {
            DecimalFormat format = new DecimalFormat("0." + "#".repeat(Math.max(0, scale))); // Return value with all decimals excluding zero
            return format.format(doubleValue);
        }

    }

    public static Boolean isEnclosedInBrackets(String input) {

        return (input.startsWith("{") && input.endsWith("}")) ||
                (input.startsWith("[") && input.endsWith("]"));

    }

    public static Document replaceSearchStringsInKeys(Document ivmAndImmData, String searchString, String replacement) {

        Document updatedDocument = new Document();
        for (Map.Entry<String, Object> entry : ivmAndImmData.entrySet()) {

            String key = entry.getKey();
            Object value = ivmAndImmData.get(key);  // as entry.value() is saved in the HashMap format, when input has JsonNode or doc values

            if (value instanceof JsonObject || value instanceof Document || value instanceof HashMap) {
                Document modifiedValue = objectMapper.convertValue(value, Document.class);
                value = replaceSearchStringsInKeys(modifiedValue, searchString, replacement);
            }

            String modifiedKey = key.replace(searchString, replacement);
            updatedDocument.put(modifiedKey, value);

            if (!Objects.equals(modifiedKey, key)) {
                updatedDocument.remove(key);
            }

        }

        return updatedDocument;
    }

    public static Document replaceSearchStringInValues(Document ivmAndImmData, String searchString, String replacement) {

        Document updatedDocument = new Document(ivmAndImmData);
        for (Map.Entry<String, Object> entry : ivmAndImmData.entrySet()) {

            String key = entry.getKey();
            Object value = ivmAndImmData.get(key);

            if (value instanceof JsonObject || value instanceof Document || value instanceof HashMap) {
                Document modifiedValue = objectMapper.convertValue(value, Document.class);
                value = replaceSearchStringInValues(modifiedValue, searchString, replacement);
                updatedDocument.append(key, value);
            } else if (Objects.nonNull(value) && value instanceof String) {
                updatedDocument.append(key, value.toString().replace(searchString, replacement));
            }

        }

        return updatedDocument;
    }

    public static String removeCommaTrailingNonBreakingSpaces(String input) {

        return input.replaceAll(TRAILING_NON_BREAKING_SPACES_COMMA_REMOVER_REGEX, "");

    }
}
