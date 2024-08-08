package com.github.srilaxmi.filereader.validations;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.srilaxmi.filereader.util.DataTypeUtil.isValueNumberType;

public class NumberValidator implements Validator {

    protected String dataTypeFormat;

    public NumberValidator(String dataTypeFormat) {

        this.dataTypeFormat = dataTypeFormat;
    }

    public boolean isValid(Object object, Boolean strictCheck) {

        if (!StringUtils.isEmpty(dataTypeFormat) && Objects.nonNull(object)) {

            String value = object.toString();
            Pattern p = Pattern.compile(dataTypeFormat);
            Matcher m = p.matcher(value);
            return m.find();

        } else {
            return isValueNumberType(object, strictCheck);
        }
    }
}
