package com.github.srilaxmi.filereader.util;

import lombok.experimental.UtilityClass;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.github.srilaxmi.filereader.constants.GlobalConstants._ID;


@UtilityClass
public class MongoQueryUtil {

    public GroupOperation generateGroupOperation(String groupField, List<String> fields) {

        GroupOperation groupOperation = Aggregation.group(groupField);

        for (String field: fields) {
            if (!Objects.equals(field, _ID) && !Objects.equals(field, groupField)) {
                groupOperation = groupOperation.addToSet("$" + field).as(field);
            }
        }

        return groupOperation;
    }

    public static String[] getProjectFields(String[] includeFields) {

        String[] projectFields = new String[includeFields.length];

        int index = 0;

        for (String includeField : List.of(includeFields)) {
            projectFields[index++] = "$" + _ID + "." + includeField;
        }
        return projectFields;
    }


}
