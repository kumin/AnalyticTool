package com.vcc.bigdata;

import com.google.common.collect.ImmutableList;
import com.vcc.bigdata.campaign.Campaign;
import com.vcc.bigdata.campaign.CampaignRepo;
import com.vcc.bigdata.campaign.ElasticCampaignRepo;
import com.vcc.bigdata.common.config.Configuration;
import com.vcc.bigdata.condition.AdvanceCondition;
import com.vcc.bigdata.condition.GroupCondition;
import com.vcc.bigdata.condition.InteractCondition;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: kumin on 02/07/2018
 **/

public class CampaignTest {
    public static void main(String[] args) {
        CampaignRepo er = new ElasticCampaignRepo(new Configuration());
        Campaign campaign = new Campaign();
        campaign.setName("shop");
        campaign.setStatus(Campaign.NON_PROCESS);
        campaign.setCreatedDate(new Date());
        campaign.setProfileType("person");

//        List<BasicCondition> bsConditions1 = new ArrayList<>();
//        GroupCondition<BasicCondition>  group1 = new GroupCondition<>();
//        BasicCondition bsCondition1 = new BasicCondition();
//
//        bsCondition1.setBool("should");
//        bsCondition1.setField("attr.name");
//        bsCondition1.setValue("fashion");
//        bsCondition1.setQuery(BasicCondition.MATCH_PHRASE);
//        bsConditions1.add(bsCondition1);
//        group1.setBool(GroupCondition.AND);
//        group1.setConditions(bsConditions1);
//
//
//        List<BasicCondition> bsConditions2 = new ArrayList<>();
//        GroupCondition<BasicCondition> group2 = new GroupCondition<>();
//        BasicCondition bsCondition2 = new BasicCondition();
//        bsCondition2.setBool("must");
//        bsCondition2.setField("website");
//        bsCondition2.setQuery(BasicCondition.EXIST);
//        bsConditions2.add(bsCondition2);
//
//        BasicCondition bsCondition3 = new BasicCondition();
//        bsCondition3.setBool("must");
//        bsCondition3.setField("address.id");
//        bsCondition3.setValue("hà nội");
//        bsCondition3.setQuery(BasicCondition.MATCH_PHRASE);
//        bsConditions2.add(bsCondition3);
//
//        group2.setBool(GroupCondition.AND);
//        group2.setConditions(bsConditions2);

        GroupCondition<AdvanceCondition> group3 = new GroupCondition<>();
        List<AdvanceCondition> adConditions = new ArrayList<>();
        InteractCondition adCondition = new InteractCondition();
        adCondition.setBool(InteractCondition.MUST);
        adCondition.setSource(InteractCondition.FANPAGE);
        adCondition.setType(InteractCondition.LIKE);
        adCondition.setValue("696877660340515");
        adCondition.setName(AdvanceCondition.INTERACT);
        adConditions.add(adCondition);
        group3.setBool(GroupCondition.AND);
        group3.setConditions(adConditions);
        campaign.setGroupAdCondition(ImmutableList.of(group3));

        er.addCampaign(campaign);
    }
}
