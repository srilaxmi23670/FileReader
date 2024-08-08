package com.github.srilaxmi.filereader.constants;

import org.springframework.data.mongodb.core.query.Criteria;

public interface OperatorStrategy {

    Criteria createCriteria(String field, Object value);

    /**
     *
     * @param lhsValue: input value
     * @param rhsValue: value of filterRule
     * @return true if condition is satisfied, else false
     */
    Boolean apply(Object lhsValue, Object rhsValue);

}