package com.vcc.bigdata.condition.escondition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.vcc.bigdata.condition.Condition;

import java.util.List;

/**
 * @author: kumin on 05/07/2018
 **/

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class GroupCondition <T extends Condition>{
    public static final String AND = "and";
    public static final String OR = "or";

    private String bool;
    private List<T> conditions;

    public GroupCondition(List<T> conditions){
        this.conditions = conditions;
    }
    public GroupCondition(){

    }
    public String getBool() {
        return bool;
    }

    public void setBool(String bool) {
        this.bool = bool;
    }

    public List<T> getConditions() {
        return conditions;
    }

    public void setConditions(List<T> conditions) {
        this.conditions = conditions;
    }
}
