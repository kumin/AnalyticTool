package com.vcc.bigdata.campaign;

import com.vcc.bigdata.common.config.Configuration;
import com.vcc.bigdata.common.config.Properties;
import com.vcc.bigdata.common.lifecycle.LoopableLifeCycle;
import com.vcc.bigdata.common.tasks.TaskManager;
import com.vcc.bigdata.common.utils.ThreadPool;
import com.vcc.bigdata.common.utils.Threads;
import com.vcc.bigdata.common.utils.Utils;
import com.vcc.bigdata.condition.escondition.AdvanceCondition;
import com.vcc.bigdata.condition.escondition.BasicCondition;
import com.vcc.bigdata.condition.escondition.GroupCondition;
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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * @author: kumin on 03/07/2018
 **/

public class CampaignExecutor extends LoopableLifeCycle {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private CampaignRepo campaignRepo;
    private Properties props;
    private ExecutorService executor;
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
        executor = ThreadPool.builder()
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

    @Override
    protected void onStop() {
        logger.info("Waiting for all workers stopped...");
        Threads.stopThreadPool(executor, 5, TimeUnit.SECONDS);
    }
    /**
     * Just execute query on elasticsearch
     *
     * @param campaign
     */
    private void queryCampaign(Campaign campaign) {
        try {
            /*
            update campaign status
             */
            Campaign campaignPrcessing = new Campaign();
            campaignPrcessing.setStatus(Campaign.PROCESSING);
            campaignPrcessing.setHost(Utils.getHostName());
            campaignRepo.updateCampaign(campaign.id(), campaignPrcessing).get();

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
              Query profile
             */
            if (mainQuery.hasClauses()) {
                queryBasicConditionHasClause(mainQuery, campaign, mustConditions, shouldConditions);
            } else queryBasicConditionHasNoClause(campaign, mustConditions, shouldConditions);

            Campaign campaignProcessed = new Campaign();
            campaignProcessed.setStatus(Campaign.PROCESSED);
            campaignRepo.updateCampaign(campaign.id(), campaignProcessed).get();
        } catch (Throwable t) {
            t.printStackTrace();
            if (isCanceled()){
                Campaign campaignProcessed = new Campaign();
                campaignProcessed.setStatus(Campaign.PROCESSED);
                campaignRepo.updateCampaign(campaign.id(), campaignProcessed);

                throw  new RuntimeException("Job canceled by user!");
            }
        }
    }

    /**
     * Elastic without join so have to check every advance condition group MUST
     * @param groupConditions
     * @param id
     * @return
     */
    public boolean checkAdvanceConditionMust(List<GroupCondition<AdvanceCondition>> groupConditions, String id) {
        boolean checkMust = true;
        boolean checkShould = true;

        for (GroupCondition<AdvanceCondition> group : groupConditions) {
            for (AdvanceCondition condition : group.getConditions()) {
                if (condition.getBool().equals(AdvanceCondition.MUST))
                    checkMust = checkMust && condition.checkConditionMust(bulkInsert.client(), id);
                else checkShould = checkShould && condition.checkConditionMust(bulkInsert.client(), id);
            }
        }
        return (checkMust || checkShould);
    }

    /**
     * With advance condition OR just insert into campaign's profile
     * @param groupConditions
     * @param campaign
     */
    public void solveAdvanceConditionOr(List<GroupCondition<AdvanceCondition>> groupConditions, Campaign campaign) {
        List<AdvanceCondition> mustConditions = new ArrayList<>();
        List<AdvanceCondition> shouldConditions = new ArrayList<>();

        groupConditions.forEach(group -> {
            for (AdvanceCondition condition : group.getConditions()) {
                if (condition.getBool().equals(AdvanceCondition.MUST)) mustConditions.add(condition);
                else shouldConditions.add(condition);
            }

            shouldConditions.forEach(condition -> condition.saveResultOr(bulkInsert, bulkSize
                    , ElasticConstant.PREFIX_CAMPAIGN_INDEX + campaign.getName() + "-" + campaign.id()
                    , getQueryTypeProfile(campaign)));

            if (mustConditions.size() == 0) return;
            AdvanceCondition firstCondition = mustConditions.get(0);
            mustConditions.remove(0);
            SearchResponse sr = firstCondition.getSatifyGuids(bulkInsert.client());
            saveResultConditionMust(sr, Collections.singletonList(new GroupCondition<>(mustConditions)), campaign);
        });
    }

    /**
     *
     * @param mainQuery
     * @param campaign
     */
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

    public QueryBuilder getQueryTypeProfile(Campaign campaign) {
        if (campaign.getProfileType().equals("org")) {
            return QueryBuilders.boolQuery()
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "org"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbpage"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "fbgroup"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "school"))
                    .should(QueryBuilders.matchPhraseQuery("attr.tag", "company"));
        } else return QueryBuilders.matchPhraseQuery("attr.tag", "person");
    }

    public void queryBasicConditionHasClause(BoolQueryBuilder mainQuery, Campaign campaign,
                                             List<GroupCondition<AdvanceCondition>> mustConditions,
                                             List<GroupCondition<AdvanceCondition>> shouldConditions) {
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
                if (checkAdvanceConditionMust(mustConditions, hit.getId())) {
                    Map<String, Object> source = hit.getSource();
                    source.put("inserted_time",
                            Collections.singletonList(Collections.singletonMap("value",new Date())));
                    bulkInsert.addRequest(ElasticConstant.PREFIX_CAMPAIGN_INDEX
                                    + campaign.getName() + "-"
                                    + campaign.id()
                            , "profiles"
                            , hit.id()
                            , source);

                    if (bulkInsert.bulkSize() > bulkSize) {
                        BulkResponse response = bulkInsert.submitBulk();
                        logger.info(Thread.currentThread().getName() + " - Submit bulk took "
                                + response.getTook().getSecondsFrac());
                    }
                }
            }
            sr = bulkInsert.client().prepareSearchScroll(sr.getScrollId()).setScroll(new TimeValue(6000000))
                    .execute().actionGet();
            if (sr.getHits().getHits().length == 0) break;
        }

        solveAdvanceConditionOr(shouldConditions, campaign);
    }

    public void queryBasicConditionHasNoClause(Campaign campaign,
                                               List<GroupCondition<AdvanceCondition>> mustConditions,
                                               List<GroupCondition<AdvanceCondition>> shouldConditions) {
        logger.info("query advance condition (has no basic condition)");
        solveAdvanceConditionOr(shouldConditions, campaign);

        /*
        deal with advance condition group must
        It's really hard job.
         */

        if (mustConditions.isEmpty()) return;
        List<AdvanceCondition> musts = new ArrayList<>();
        List<AdvanceCondition> shoulds = new ArrayList<>();

        GroupCondition<AdvanceCondition> firstGroup = mustConditions.get(0);
        mustConditions.remove(0);
        firstGroup.getConditions().forEach(condition -> {
            if (condition.getBool().equals(AdvanceCondition.MUST)) musts.add(condition);
            else shoulds.add(condition);
        });

        shoulds.forEach(condition -> {
            SearchResponse sr = condition.getSatifyGuids(bulkInsert.client());
            saveResultConditionMust(sr, mustConditions, campaign);
        });
        if (musts.isEmpty()) return;
        AdvanceCondition firstMustCondition = musts.get(0);
        musts.remove(0);
        mustConditions.add(new GroupCondition<>(musts));
        SearchResponse sr = firstMustCondition.getSatifyGuids(bulkInsert.client());
        saveResultConditionMust(sr, mustConditions, campaign);
    }

    public void saveResultConditionMust(SearchResponse sr
            , List<GroupCondition<AdvanceCondition>> groups
            , Campaign campaign) {
        logger.info("save result with condition must");
        while (isNotCanceled()) {
            for (SearchHit hit : sr.getHits()) {
                String hitId = hit.id().split("_")[0];
                if (checkAdvanceConditionMust(groups, hit.getId())) {
                    SearchHit hit1 = AdvanceCondition.getProfile(bulkInsert.client(), hitId
                            , getQueryTypeProfile(campaign));

                    if (hit1 != null) {
                        Map<String, Object> source = hit1.getSource();
                        source.put("inserted_time",
                                Collections.singletonList(Collections.singletonMap("value",new Date())));
                        bulkInsert.addRequest(ElasticConstant.PREFIX_CAMPAIGN_INDEX
                                        + campaign.getName() + "-"
                                        + campaign.id()
                                , ElasticConstant.PROFILES_TYPE
                                , hit1.id()
                                , source);

                        if (bulkInsert.bulkSize() > bulkSize) {
                            BulkResponse response = bulkInsert.submitBulk();
                            logger.info(Thread.currentThread().getName() + " - Submit bulk took "
                                    + response.getTook().getSecondsFrac());
                        }
                    }
                }
            }
            sr = bulkInsert.client().prepareSearchScroll(sr.getScrollId()).setScroll(new TimeValue(6000000))
                    .execute().actionGet();
            if (sr.getHits().getHits().length == 0) break;
        }
    }

    public static void main(String[] args) {
        Properties props = new Configuration().toSubProperties("analytic-tool");
        CampaignExecutor ce = new CampaignExecutor(props);
        ce.start();
    }
}
