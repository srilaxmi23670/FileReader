package com.github.srilaxmi.filereader.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.github.srilaxmi.filereader.util.DataTypeConversionUtil;
import com.github.srilaxmi.filereader.util.DataTypeUtil;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.EMPTY_STRING;
import static com.github.srilaxmi.filereader.constants.GlobalConstants.FILTER_QUERY_REGEX;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public enum Operator implements OperatorStrategy {

    EQUALS("=") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).is(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return Objects.equals(lhsValue, rhsValue);
        }
    },
    NOT_EQUALS("!=") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).ne(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return !Objects.equals(lhsValue, rhsValue);
        }
    },
    CONTAINS("contains") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).regex(replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value)), GlobalConstants.CASE_INSENSITIVE);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(lhsValue) && Objects.nonNull(rhsValue)) {
                String lhs = Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString();
                String rhs = Objects.requireNonNullElse(rhsValue, EMPTY_STRING).toString();
                return lhs.contains(rhs);
            } else {
                return Boolean.FALSE;
            }
        }
    },
    NOT_CONTAINS("not contains") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).not().regex(replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value)), GlobalConstants.CASE_INSENSITIVE);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(lhsValue) && Objects.nonNull(rhsValue)) {
                String lhs = Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString();
                String rhs = Objects.requireNonNullElse(rhsValue, EMPTY_STRING).toString();
                return !lhs.contains(rhs);
            } else {
                return Boolean.TRUE;
            }
        }
    },
    NOT_EMPTY("not empty") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).nin(null, EMPTY_STRING);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return Objects.nonNull(lhsValue) && !Objects.equals(lhsValue, EMPTY_STRING);
        }
    },
    IS_EMPTY("is empty") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).isNull();
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return Objects.isNull(lhsValue) || Objects.equals(lhsValue, EMPTY_STRING);
        }
    },
    STARTS_WITH("starts with") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            Pattern pattern = Pattern.compile("^" + replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value)), CASE_INSENSITIVE);
            return Criteria.where(field).regex(pattern);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(rhsValue)) {
                Pattern pattern = Pattern.compile("^" + rhsValue, CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString());
                return matcher.find();
            } else {
                return Boolean.FALSE;
            }

        }
    },
    ENDS_WITH("ends with") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            String regex = replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value)) + "$";
            Pattern pattern = Pattern.compile(regex, CASE_INSENSITIVE);
            return Criteria.where(field).regex(pattern);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(rhsValue)) {
                Pattern pattern = Pattern.compile(rhsValue + "$", CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString());
                return matcher.find();
            } else {
                return Boolean.FALSE;
            }

        }
    },
    NOT_STARTS_WITH("not starts with") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            String regex = "^" + replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value));
            Pattern pattern = Pattern.compile(regex, CASE_INSENSITIVE);
            return Criteria.where(field).not().regex(pattern);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(rhsValue)) {
                Pattern pattern = Pattern.compile("^" + rhsValue, CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString());
                return !matcher.find();
            } else {
                return Boolean.TRUE;
            }
        }
    },
    NOT_ENDS_WITH("not ends with") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            String regex = replaceSpecialCharacterWithEscapeSpecialCharacter(String.valueOf(value)) + "$";
            Pattern pattern = Pattern.compile(regex, CASE_INSENSITIVE);
            return Criteria.where(field).not().regex(pattern);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (Objects.nonNull(rhsValue)) {
                Pattern pattern = Pattern.compile(rhsValue + "$", CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(Objects.requireNonNullElse(lhsValue.toString(), EMPTY_STRING));
                return !matcher.find();
            } else {
                return Boolean.TRUE;
            }
        }

    },
    GREATER_THAN(">") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            value = DataTypeConversionUtil.convertToNumberOrDateOrGetAsIs(value);
            return Criteria.where(field).gt(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return compare(lhsValue, rhsValue, GREATER_THAN);
        }
    },
    LESS_THAN("<") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            value = DataTypeConversionUtil.convertToNumberOrDateOrGetAsIs(value);
            return Criteria.where(field).lt(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return compare(lhsValue, rhsValue, LESS_THAN);
        }
    },
    GREATER_THAN_EQUALS(">=") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            value = DataTypeConversionUtil.convertToNumberOrDateOrGetAsIs(value);
            return Criteria.where(field).gte(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return compare(lhsValue, rhsValue, GREATER_THAN_EQUALS);
        }
    },
    LESS_THAN_EQUALS("<=") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            value = DataTypeConversionUtil.convertToNumberOrDateOrGetAsIs(value);
            return Criteria.where(field).lte(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {
            return compare(lhsValue, rhsValue, LESS_THAN_EQUALS);
        }
    },
    IS_IN("in") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).in(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (rhsValue instanceof List) {
                List<?> rhsValues = (List<?>) rhsValue;
                return rhsValues.contains(lhsValue);
            } else {
                return Boolean.FALSE;
            }
        }
    },
    IS_NOT_IN("not in") {
        @Override
        public Criteria createCriteria(String field, Object value) {
            return Criteria.where(field).nin(value);
        }

        @Override
        public Boolean apply(Object lhsValue, Object rhsValue) {

            if (rhsValue instanceof List) {
                List<?> rhsValues = (List<?>) rhsValue;
                return !rhsValues.contains(lhsValue);
            } else {
                return Boolean.TRUE;
            }
        }
    };

    private final String code;

    Operator(String code) {
        this.code = code;
    }

    @JsonValue
    public String getCode() {
        return code;
    }

    @JsonCreator
    public static Operator decode(final String code) {
        return Stream.of(Operator.values())
                .filter(targetEnum -> targetEnum.code.equals(code))
                .findFirst()
                .orElse(null);
    }

    /**
     * @return comparison result by typeCasting both lhs and rhs to the same instance as of lhsValue
     */
    private static Boolean compare(Object lhsValue, Object rhsValue, Operator operator) {

        if (Objects.nonNull(rhsValue)) {
            return compareValuesByDataType(lhsValue, rhsValue, operator);
        } else {
            return Boolean.FALSE;
        }

    }

    private static Boolean compareValuesByDataType(Object lhsValue, Object rhsValue, Operator operator) {

        if (DataTypeUtil.isValueNumberType(lhsValue, true)) {

            return compareIfLHSIsNumberType(lhsValue, rhsValue, operator);

        } else {

            return compareIfLHSIsNotNumberType(lhsValue, rhsValue, operator);
        }

    }

    private static Boolean compareIfLHSIsNumberType(Object lhsValue, Object rhsValue, Operator operator) {

        Object modifiedLhs = Objects.requireNonNullElse(lhsValue, 0).toString();
        Double lhs = Double.parseDouble(modifiedLhs.toString());
        Double rhs = Double.parseDouble(rhsValue.toString());

        return compareLHSAndRHS(operator, lhs, rhs);

    }


    private static Boolean compareIfLHSIsNotNumberType(Object lhsValue, Object rhsValue, Operator operator) {

        String lhs = Objects.requireNonNullElse(lhsValue, EMPTY_STRING).toString();
        String rhs = Objects.requireNonNullElse(rhsValue, EMPTY_STRING).toString();

        return compareLHSAndRHS(operator, lhs, rhs);

    }

    private static <T extends Comparable<T>>  Boolean compareLHSAndRHS(Operator operator, T lhs, T rhs) {

        if (LESS_THAN.equals(operator)) {
            return lhs.compareTo(rhs) < 0;
        } else if (GREATER_THAN.equals(operator)) {
            return lhs.compareTo(rhs) > 0;
        } else if (LESS_THAN_EQUALS.equals(operator)) {
            return lhs.compareTo(rhs) <= 0;
        } else if (GREATER_THAN_EQUALS.equals(operator)) {
            return lhs.compareTo(rhs) >= 0;
        } else {
            return Boolean.FALSE;
        }

    }

    public String replaceSpecialCharacterWithEscapeSpecialCharacter(String value) {

        return value.replaceAll(FILTER_QUERY_REGEX, "\\\\$0");
    }

}
