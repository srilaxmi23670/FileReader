package com.github.srilaxmi.filereader.validations;

import org.springframework.util.CollectionUtils;

import java.util.List;

public class BooleanValidator implements Validator{

    @Override
    public boolean isValid(Object object, Boolean strictCheck) {

        try {
            if (strictCheck) {
                return CollectionUtils.containsAny(List.of(true, false, Boolean.TRUE, Boolean.FALSE), List.of(object));
            } else {
                return "TRUE".equalsIgnoreCase(object.toString()) || "FALSE".equalsIgnoreCase(object.toString());
            }
        } catch (NullPointerException npe) {
            return false;
        }
    }
}
