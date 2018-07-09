package com.vcc.bigdata.campaign;

import com.vcc.bigdata.common.config.Configuration;
import com.vcc.bigdata.common.config.Properties;
import com.vcc.bigdata.common.lifecycle.LoopableLifeCycle;
import com.vcc.bigdata.common.tasks.TaskManager;
import com.vcc.bigdata.common.utils.ThreadPool;
import com.vcc.bigdata.condition.AdvanceCondition;
import com.vcc.bigdata.condition.BasicCondition;
import com.vcc.bigdata.condition.GroupCondition;
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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;


/**
 * @author: kumin on 03/07/2018
 **/

public class CampaignExecutor extends LoopableLifeCycle {

    private static final String ES_INDEX = "graph-profiles";
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
            campaign.getGroupBsConditions().forEach(conditions -> {
                BoolQueryBuilder bsBoolQueries = new BoolQueryBuilder();
                conditions.getConditions().forEach(condition -> {
                    QueryBuilder queryBuilder = condition.generateQuery();
                    switch (condition.getBool()) {
                        case BasicCondition.SHOULD:
                            bsBoolQueries.must(QueryBuilders.boolQuery().should(queryBuilder));
                            break;
                        case BasicCondition.MUST:
                            bsBoolQueries.must(queryBuilder);
                            break;
                        case BasicCondition.MUST_NOT:
                            bsBoolQueries.mustNot(queryBuilder);
                            break;
                    }
                });
                switch (conditions.getBool()){
                    case GroupCondition.AND:
                        mainQuery.must(bsBoolQueries);
                        break;
                    case GroupCondition.OR:
                        mainQuery.should(bsBoolQueries);
                        break;
                }
            });

            /*
            Get advance query
             */
            campaign.getGroupAdCondition().forEach(conditions ->{
                BoolQueryBuilder adBoolQueries = new BoolQueryBuilder();
                conditions.getConditions().forEach(condition -> {
                    switch (condition.getBool()){
                        case AdvanceCondition.MUST:
                            adBoolQueries.must(condition.generateQuery());
                            break;
                        case AdvanceCondition.SHOULD:
                            adBoolQueries.should(condition.generateQuery());
                            break;
                    }
                });
                switch (conditions.getBool()){
                    case GroupCondition.AND:
                        mainQuery.must(adBoolQueries);
                        break;
                    case GroupCondition.OR:
                        mainQuery.should(adBoolQueries);
                        break;
                }
            });

            /*
              Query type of profile
             */
            if (campaign.getProfileType().equals("org")) {
                mainQuery.must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchPhraseQuery("attr.tag", "org"))
                        .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbpage"))
                        .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbgroup"))
                        .should(QueryBuilders.matchPhraseQuery("attr.tag", "school"))
                        .should(QueryBuilders.matchPhraseQuery("attr.tag", "company")));
            } else mainQuery.must(QueryBuilders.matchPhraseQuery("attr.tag", "person"));
            System.out.println(mainQuery);

            SearchResponse sr = bulkInsert.client().prepareSearch(ES_INDEX)
                    .setTypes("profiles")
                    .setQuery(mainQuery)
                    .setScroll(new TimeValue(6000000))
                    .setSize(1000)
                    .execute()
                    .actionGet();

            while (isNotCanceled()) {
                for (SearchHit hit : sr.getHits()) {
                    bulkInsert.addRequest("datacollection-campaign-" + campaign.getName() + "-" + campaign.id()
                            , "profile"
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
            Campaign campaignProcessed = new Campaign();
            campaignProcessed.setStatus(Campaign.PROCESSED);
            campaignRepo.updateCampaign(campaign.id(), campaignProcessed).get();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties props = new Configuration().toSubProperties("analytic-tool");
        CampaignExecutor ce = new CampaignExecutor(props);
        ce.start();
    }
}
