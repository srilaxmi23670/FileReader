package com.github.srilaxmi.filereader.constants;

import com.github.srilaxmi.filereader.util.DataTypeConversionUtil;
import com.github.srilaxmi.filereader.validations.*;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.*;

public enum DataType implements DataTypeStrategy {

    NUMBER {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN_EQUALS,
                    Operator.LESS_THAN_EQUALS);
        }

        @Override
        public Object convert(Object value) {

            return convertToNumber(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {

            Validator numberValidator = new NumberValidator(validationRegex);
            return numberValidator.isValid(value, strictCheck);

        }
    },
    BOOLEAN {

        @Override
        public List<Operator> getOperators() {
            return List.of(Operator.EQUALS);
        }

        @Override
        public Object convert(Object value) {
            return convertToBoolean(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator booleanValidator = new BooleanValidator();
            return booleanValidator.isValid(value, strictCheck);
        }
    },
    JSON {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY);
        }

        @Override
        public Object convert(Object value) {
            return value;
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator jsonValidator = new JsonValidator();
            return jsonValidator.isValid(value, strictCheck);
        }
    },
    DOCUMENT {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY);
        }

        @Override
        public Object convert(Object value) {
            return DataTypeConversionUtil.convertToDocument(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator documentValidator = new DocumentValidator();
            return documentValidator.isValid(value, strictCheck);
        }
    },
    NUMBER_ARRAY {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY,
                    Operator.IS_IN,
                    Operator.IS_NOT_IN);
        }

        @Override
        public Object convert(Object value) {

            return convertToNumberArray(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator numberArrayValidator = new NumberArrayValidator();
            return numberArrayValidator.isValid(value, strictCheck);
        }
    },
    STRING_ARRAY {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY,
                    Operator.IS_IN,
                    Operator.IS_NOT_IN);
        }

        @Override
        public Object convert(Object value) {
            return convertToStringArray(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator stringArrayValidator = new StringArrayValidator();
            return stringArrayValidator.isValid(value, strictCheck);
        }
    },
    DATE {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY);
        }

        @Override
        public Object convert(Object value) {
            return convertToDate(value);
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            Validator dateValidator = new DateValidator(DateTimeFormatter.ISO_INSTANT);
            return dateValidator.isValid(value, strictCheck);
        }
    },
    STRING {

        @Override
        public List<Operator> getOperators() {
            return Arrays.asList(
                    Operator.CONTAINS,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.NOT_CONTAINS,
                    Operator.NOT_EMPTY,
                    Operator.IS_EMPTY,
                    Operator.STARTS_WITH,
                    Operator.ENDS_WITH,
                    Operator.NOT_STARTS_WITH,
                    Operator.NOT_ENDS_WITH);
        }

        @Override
        public Object convert(Object value) {
            return value.toString();
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            StringValidator stringValidator = new StringValidator(validationRegex);
            return stringValidator.isValid(value, strictCheck);
        }
    },
    GEOCODE {
        @Override
        public List<Operator> getOperators() {
            return Collections.singletonList(
                    Operator.EQUALS);
        }

        @Override
        public Object convert(Object value) {
            return value;
        }

        @Override
        public Boolean isValid(Object value, String validationRegex, Boolean strictCheck) {
            return true;
        }
    }

}