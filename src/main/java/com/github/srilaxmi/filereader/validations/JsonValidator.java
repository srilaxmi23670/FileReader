package com.github.srilaxmi.filereader.validations;


import com.github.tsohr.JSONObject;

public class JsonValidator implements Validator {

    @Override
    public boolean isValid(Object input, Boolean strictCheck) {

        try {
            Object value = strictCheck ?
                    new JSONObject(input.toString()) :
                    false;
            return true;
        } catch (Exception e) {
            return false;
        }

    }
}