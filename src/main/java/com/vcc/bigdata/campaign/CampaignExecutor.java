package com.vcc.bigdata.campaign;

import com.vcc.bigdata.common.config.Configuration;
import com.vcc.bigdata.common.config.Properties;
import com.vcc.bigdata.common.lifecycle.LoopableLifeCycle;
import com.vcc.bigdata.common.tasks.TaskManager;
import com.vcc.bigdata.common.utils.ThreadPool;
import com.vcc.bigdata.condition.AdvanceCondition;
import com.vcc.bigdata.condition.BasicCondition;
import com.vcc.bigdata.condition.GroupCondition;
import com.vcc.bigdata.model.ElasticConstant;
import com.vcc.bigdata.platform.elastic.ElasticBulkInsert;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;


/**
 * @author: kumin on 03/07/2018
 **/

public class CampaignExecutor extends LoopableLifeCycle {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private CampaignRepo campaignRepo;
    private Properties props;
    private TaskManager taskManager;
    private ElasticBulkInsert bulkInsert;
    private int bulkSize;

    public CampaignExecutor(Properties props) {
        this.props = props;
    }

    @Override
    protected void onInitialize() {
        campaignRepo = new ElasticCampaignRepo(this.props);
        bulkSize = props.getIntProperty("bulkSize", 5);
        int nThread = props.getIntProperty("nthread", Runtime.getRuntime().availableProcessors());
        ExecutorService executor = ThreadPool.builder()
                .setCoreSize(nThread)
                .setQueueSize(10)
                .setNamePrefix("query campaign")
                .build();

        taskManager = new TaskManager(props, executor);

        bulkInsert = new ElasticBulkInsert(props);
    }

    @Override
    protected void onLoop() throws Exception {
        List<Campaign> campaigns = campaignRepo.getCampaigns();
        logger.info("get " + campaigns.size() + " campaign!");
        for (Campaign campaign : campaigns) {
            while (isNotCanceled()) {
                try {
                    taskManager.submit(() -> queryCampaign(campaign));
                    break;
                } catch (RejectedExecutionException ignore) {

                }
            }
        }
    }

    /**
     * Just execute query on elasticsearch
     *
     * @param campaign
     */
    private void queryCampaign(Campaign campaign) {
        try {
            BoolQueryBuilder mainQuery = new BoolQueryBuilder();

            /*
            Get Basic Queries
             */
            getQueryBasicCondition(mainQuery, campaign);

            /*
            Get advance query
             */
            List<GroupCondition<AdvanceCondition>> mustConditions = new ArrayList<>();
            List<GroupCondition<AdvanceCondition>> shouldConditions = new ArrayList<>();
            campaign.getGroupAdCondition().forEach(group -> {
                if (group.getBool().equals(GroupCondition.AND)) mustConditions.add(group);
                else shouldConditions.add(group);
            });

            /*
              Query type of profile
             */
            if (mainQuery.hasClauses()) {
                queryBasicConditionHasClause(mainQuery, campaign, mustConditions, shouldConditions);
            }
            else queryBasicConditionHasNoClause(campaign, mustConditions, shouldConditions);
            Campaign campaignProcessed = new Campaign();
            campaignProcessed.setStatus(Campaign.PROCESSED);
            campaignRepo.updateCampaign(campaign.id(), campaignProcessed).get();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public boolean checkAdvanceConditionMust(List<GroupCondition<AdvanceCondition>> groupConditions, String id) {
        boolean check = true;
        for (GroupCondition<AdvanceCondition> group : groupConditions) {
            for (AdvanceCondition condition : group.getConditions()) {
                if (condition.getBool().equals(AdvanceCondition.MUST))
                    check = check && condition.checkConditionMust(bulkInsert.client(), id);
                else check = check || condition.checkConditionMust(bulkInsert.client(), id);
            }
        }
        return check;
    }

    public void solveAdvanceConditionOr(List<GroupCondition<AdvanceCondition>> groupConditions, String index) {
        List<AdvanceCondition> mustConditions = new ArrayList<>();
        List<AdvanceCondition> shouldConditions = new ArrayList<>();

        groupConditions.forEach(group -> {
            for (AdvanceCondition condition : group.getConditions()) {
                if (condition.getBool().equals(AdvanceCondition.MUST)) mustConditions.add(condition);
                else shouldConditions.add(condition);
            }

            shouldConditions.forEach(condition -> {
                condition.saveResultOr(bulkInsert, bulkSize, index);
            });


        });


    }

    public void getQueryBasicCondition(BoolQueryBuilder mainQuery, Campaign campaign) {
        campaign.getGroupBsConditions().forEach(conditions -> {
            BoolQueryBuilder bsBoolQueries = new BoolQueryBuilder();
            conditions.getConditions().forEach(condition -> {
                QueryBuilder queryBuilder = condition.generateQuery();
                switch (condition.getBool()) {
                    case BasicCondition.SHOULD:
                        bsBoolQueries.should(queryBuilder);
                        break;
                    case BasicCondition.MUST:
                        bsBoolQueries.must(queryBuilder);
                        break;
                    case BasicCondition.MUST_NOT:
                        bsBoolQueries.mustNot(queryBuilder);
                        break;
                }
            });
            switch (conditions.getBool()) {
                case GroupCondition.AND:
                    mainQuery.must(bsBoolQueries);
                    break;
                case GroupCondition.OR:
                    mainQuery.should(bsBoolQueries);
                    break;
            }
        });
    }

    public void getQueryTypeProfile(BoolQueryBuilder mainQuery, Campaign campaign) {
        if (campaign.getProfileType().equals("org")) {
            mainQuery.must(QueryBuilders.boolQuery()
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "org"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbpage"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbgroup"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "school"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "company")));
        } else mainQuery.must(QueryBuilders.matchPhraseQuery("attr.tag", "person"));
    }

    public void queryBasicConditionHasClause(BoolQueryBuilder mainQuery, Campaign campaign,
                                             List<GroupCondition<AdvanceCondition>> mustConditions,
                                             List<GroupCondition<AdvanceCondition>> shouldConditions){
        getQueryTypeProfile(mainQuery, campaign);
        System.out.println(mainQuery);

        SearchResponse sr = bulkInsert.client().prepareSearch(ElasticConstant.PROFILES_INDEX)
                .setTypes("profiles")
                .setQuery(mainQuery)
                .setScroll(new TimeValue(6000000))
                .setSize(1000)
                .execute()
                .actionGet();

        while (isNotCanceled()) {
            for (SearchHit hit : sr.getHits()) {
                if (checkAdvanceConditionMust(mustConditions, hit.getId())) ;
                bulkInsert.addRequest(ElasticConstant.PREFIX_CAMPAIGN_INDEX
                                + campaign.getName() + "-"
                                + campaign.id()
                        , "profiles"
                        , hit.id()
                        , hit.getSource());

                if (bulkInsert.bulkSize() > bulkSize) {
                    BulkResponse response = bulkInsert.submitBulk();
                    logger.info(Thread.currentThread().getName() + " - Submit bulk took "
                            + response.getTook().getSecondsFrac());
                }
            }
            sr = bulkInsert.client().prepareSearchScroll(sr.getScrollId()).setScroll(new TimeValue(6000000))
                    .execute().actionGet();
            if (sr.getHits().getHits().length == 0) break;
        }

        solveAdvanceConditionOr(shouldConditions,
                ElasticConstant.PREFIX_CAMPAIGN_INDEX + campaign.getName() + "-" + campaign.id());
    }

    public void queryBasicConditionHasNoClause(Campaign campaign,
                                              List<GroupCondition<AdvanceCondition>> mustConditions,
                                              List<GroupCondition<AdvanceCondition>> shouldConditions){



    }
    public static void main(String[] args) {
        Properties props = new Configuration().toSubProperties("analytic-tool");
        CampaignExecutor ce = new CampaignExecutor(props);
        ce.start();
    }
}
