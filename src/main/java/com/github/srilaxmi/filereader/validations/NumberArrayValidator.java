package com.github.srilaxmi.filereader.validations;

import java.util.List;

import static com.github.srilaxmi.filereader.util.DataTypeConversionUtil.strictConvertToNumberArray;
import static com.github.srilaxmi.filereader.util.DataTypeUtil.isValueNumberType;

public class NumberArrayValidator implements Validator {

    @Override
    public boolean isValid(Object object, Boolean strictCheck) {

        try {
            if (object instanceof List) {
                List<?> values = (List<?>) object;
                return !values.isEmpty() &&
                        values.stream().allMatch(value -> isValueNumberType(value, strictCheck));
            } else if (!strictCheck) {
                strictConvertToNumberArray(object);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }


    }
}
