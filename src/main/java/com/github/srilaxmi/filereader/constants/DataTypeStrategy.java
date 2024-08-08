package com.github.srilaxmi.filereader.constants;

import java.util.List;

public interface DataTypeStrategy {

    List<Operator> getOperators();

    Object convert(Object value);

    Boolean isValid(Object value, String validationRegex, Boolean strictCheck);

}
