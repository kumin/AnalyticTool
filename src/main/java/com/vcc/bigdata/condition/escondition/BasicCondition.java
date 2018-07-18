package com.vcc.bigdata.condition.escondition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.vcc.bigdata.condition.Condition;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * @author: kumin on 02/07/2018
 **/

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BasicCondition implements Condition<QueryBuilder> {

    public static final String MUST = "must";
    public static final String MUST_NOT = "must_not";
    public static final String SHOULD = "should";

    public static final String MATCH = "match";
    public static final String MATCH_PHRASE = "march_phrase";
    public static final String TERM = "term";
    public static final String EXIST = "exist";

    private String bool;
    private String field;
    private String value;
    private String query;

    @Override
    public QueryBuilder generateQuery() {
        switch (query) {
            case MATCH: return QueryBuilders.matchPhraseQuery(field, value);
            case MATCH_PHRASE: return QueryBuilders.matchQuery(field, value);
            case TERM: return QueryBuilders.termQuery(field, value);
            case EXIST: return QueryBuilders.existsQuery(field);
        }
        return null;
    }

    public String getBool() {
        return bool;
    }

    public void setBool(String bool) {
        this.bool = bool;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
