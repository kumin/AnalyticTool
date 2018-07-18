package com.vcc.bigdata.campaign;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vcc.bigdata.condition.escondition.AdvanceCondition;
import com.vcc.bigdata.condition.escondition.BasicCondition;
import com.vcc.bigdata.condition.escondition.GroupCondition;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: kumin on 03/07/2018
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Campaign {
    public static final String NON_PROCESS = "1";
    public static final String PROCESSING = "2";
    public static final String PROCESSED = "3";
    public static final String DELETED = "0";

    @JsonIgnoreProperties
    private String id;

    private String name;
    @JsonProperty("profile_type")
    private String profileType;
    @JsonProperty("created_date")
    private Date createdDate;

    @JsonProperty("basic_conditions")
    private List<GroupCondition<BasicCondition>> groupBsConditions = new ArrayList<>();
    @JsonProperty("advantage_conditions")
    private List<GroupCondition<AdvanceCondition>> groupAdCondition = new ArrayList<>();

    private String status;

    public Campaign(String id) {
        this.id = id;
    }

    public Campaign() {

    }

    public String id() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProfileType() {
        return profileType;
    }

    public void setProfileType(String profileType) {
        this.profileType = profileType;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public List<GroupCondition<BasicCondition>> getGroupBsConditions() {
        return groupBsConditions;
    }

    public void setGroupBsConditions(List<GroupCondition<BasicCondition>> groupBsConditions) {
        this.groupBsConditions = groupBsConditions;
    }

    public List<GroupCondition<AdvanceCondition>> getGroupAdCondition() {
        return groupAdCondition;
    }

    public void setGroupAdCondition(List<GroupCondition<AdvanceCondition>> groupAdCondition) {
        this.groupAdCondition = groupAdCondition;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }


}
