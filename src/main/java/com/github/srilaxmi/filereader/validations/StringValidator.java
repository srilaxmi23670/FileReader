package com.github.srilaxmi.filereader.validations;

import com.github.srilaxmi.filereader.util.DataTypeUtil;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringValidator implements Validator {

    protected String dataTypeFormat;

     public StringValidator(String dataTypeFormat) {

         this.dataTypeFormat = dataTypeFormat;
    }

    @Override
    public boolean isValid(Object object, Boolean strictCheck) {

         if (Objects.nonNull(dataTypeFormat)) {
             String value = Objects.toString(object, "");
             Pattern p = Pattern.compile(dataTypeFormat);
             Matcher m = p.matcher(value);
             return m.find();
         }

         return DataTypeUtil.isValueStringType(object, strictCheck);

    }
}
