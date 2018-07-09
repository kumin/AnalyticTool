package com.vcc.bigdata.condition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * @author: kumin on 05/07/2018
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class InteractCondition extends AdvanceCondition{

    public static final String POST = "post";
    public static final String COMMENT = "comment";
    public static final String LIKE = "like";

    public static final String FANPAGE = "fanpage";
    public static final String GROUP = "group";
    public static final String FORUM = "forum";

    private String type;
    private String source;
    private String value;

    @Override
    public QueryBuilder generateQuery() {
        String interactType = getInteractType(type);
        String interactSource = getInteractSource(source);

        return QueryBuilders.matchPhraseQuery(interactType, interactSource+"_"+value);
    }

    public String getInteractType(String type){
        switch (type){
            case LIKE: return "like.id";
        }
        return null;
    }

    public String getInteractSource(String source){
        switch (source){
            case FANPAGE: return "fbpage";
            case GROUP: return "fbgroup";
            case FORUM: return "";
        }
        return null;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
