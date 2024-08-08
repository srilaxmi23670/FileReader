package com.github.srilaxmi.filereader.validations;

import java.util.List;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.strictConvertToStringArray;
import static com.github.srilaxmi.filereader.util.DataTypeUtil.isValueStringType;


public class StringArrayValidator implements Validator {

    @Override
    public boolean isValid(Object object, Boolean strictCheck) {

        try {
            if (object instanceof List) {
                List<?> values = (List<?>) object;
                return !values.isEmpty() &&
                        values.stream().allMatch(value -> isValueStringType(value, strictCheck));
            } else if (!strictCheck) {
                strictConvertToStringArray(object);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

    }

}

