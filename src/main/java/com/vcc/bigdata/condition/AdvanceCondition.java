package com.vcc.bigdata.condition;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @author: kumin on 03/07/2018
 **/

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "class")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InteractCondition.class),
})
public abstract class AdvanceCondition implements Condition<QueryBuilder>{
    public static final String MUST = "must";
    public static final String SHOULD = "should";

    public static final String INTERACT = "interact";
    public static final String COUNTER = "counter";
    public static final String CONTENT = "content";

    private String bool;
    private String name;

    public String getBool() {
        return bool;
    }

    public void setBool(String bool) {
        this.bool = bool;
    }

    public static String getMUST() {
        return MUST;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
