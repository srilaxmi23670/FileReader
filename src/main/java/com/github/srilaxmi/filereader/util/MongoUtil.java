package com.github.srilaxmi.filereader.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.srilaxmi.filereader.dto.Pair;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.*;

public class MongoUtil {

    private static final Integer ID_DISPLAY_VALUES_LIMIT = 10;

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String getIdFromMongoDocument(Map<String, Object> document) {

        String id = "";

        if (document.containsKey(_ID) && document.get(_ID) != null) {

            id = String.valueOf(document.get(_ID));

        } else if (document.containsKey(ID) && document.get(ID) != null) {

            id = String.valueOf(document.get(ID));

        }

        return id;

    }

    public static Boolean areMongoDocumentsIdentical(Document document1, Document document2) {

        document1.remove(_ID);
        document2.remove(_ID);

        return document1.equals(document2);
    }

    public static Pair<Boolean, String> getUniquenessMessagePair(List<Object> objects) {

        List<Map<String, Object>> data = objects
                .stream()
                .map(object -> {
                    Map<String, Object> map = objectMapper.convertValue(object, Map.class);
                    return map;
                })
                .filter(map -> {
                    return !map.get(AGGREGATE_TOTAL).equals(1);
                }).collect(Collectors.toList());

        if (data.isEmpty()) {
            return new Pair<Boolean, String>(true, "");
        }
        return new Pair<Boolean, String>(false, getDuplicateEntries(data));
    }

    public static String getDuplicateEntries(List<Map<String, Object>> data) {

        StringBuilder stringBuilder = new StringBuilder();
        Integer index = 0;

        for (Map<String, Object> map : data) {

            if (index.equals(ID_DISPLAY_VALUES_LIMIT) || index.equals(data.size())) {
                stringBuilder.append(" ..etc");
                break;
            }

            stringBuilder.append(map.get("_id")).append(", ");
            index++;
        }

        return stringBuilder.toString();
    }

}
