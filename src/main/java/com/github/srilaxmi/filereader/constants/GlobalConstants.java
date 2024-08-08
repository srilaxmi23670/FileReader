package com.github.srilaxmi.filereader.constants;

public class GlobalConstants {

    public static final Integer BATCH_SIZE = 5000;
    public static final String AGGREGATE_TOTAL = "total";
    public static final String ID = "id";
    public static final String _ID = "_id";
    public static final String[] HEADERS = {"name"};
    public static final String TRAILING_NON_BREAKING_SPACES_COMMA_REMOVER_REGEX = " |&nbsp;|[ \\s\\u00A0]+$|,";
    public static final String EMPTY_STRING = "";
    public static final String ISO_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String CASE_INSENSITIVE = "i";
    public final static String FILTER_QUERY_REGEX = "[\\.\\*\\+\\?\\^\\${}\\(\\)|\\]\\[\\\\]";

}
