package com.vcc.bigdata.condition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.vcc.bigdata.model.ElasticConstant;
import com.vcc.bigdata.platform.elastic.ElasticBulkInsert;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: kumin on 05/07/2018
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class InteractCondition extends AdvanceCondition {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

        return QueryBuilders.boolQuery()
                .filter(QueryBuilders.matchPhraseQuery(interactType, interactSource + "_" + value));
    }

    public String getInteractType(String type) {
        switch (type) {
            case LIKE:
                return "like.id";
            case POST:
                return "fbpid";
            case COMMENT:
                return "fbpid";
        }
        return null;
    }

    public String getInteractSource(String source) {
        switch (source) {
            case FANPAGE:
                return "fbpage";
            case GROUP:
                return "fbgroup";
            case FORUM:
                return "";
        }
        return null;
    }

    public final void getResult() {

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

    @Override
    public void saveResultOr(ElasticBulkInsert bulkInsert, int bulkSize, String index) {
        Client client = bulkInsert.client();
        if (type.equals(InteractCondition.LIKE)) {
            SearchResponse sr = client.prepareSearch(ElasticConstant.PROFILES_INDEX)
                    .setTypes(ElasticConstant.PROFILES_TYPE)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders
                                    .termQuery("like.id", getInteractSource(source) + "_" + value)))
                    .setSize(1000)
                    .setScroll(new TimeValue(6000000))
                    .execute()
                    .actionGet();
            while (true) {
                for (SearchHit hit : sr.getHits()) {
                    bulkInsert.addRequest(index, ElasticConstant.PROFILES_TYPE, hit.id(), hit.getSource());

                    if (bulkInsert.bulkSize() > bulkSize) {
                        BulkResponse response = bulkInsert.submitBulk();
                        logger.info(Thread.currentThread().getName() + " - Submit bulk took "
                                + response.getTook().getSecondsFrac());
                    }
                }

                sr = client.prepareSearchScroll(sr.getScrollId())
                        .setScroll(new TimeValue(6000000)).execute().actionGet();
                if (sr.getHits().getHits().length == 0) break;
            }
        } else {
            SearchResponse sr = client.prepareSearch(ElasticConstant.INTERACT_INDEX)
                    .setTypes(ElasticConstant.INTERACT_TYPE)
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("fbpid", value)))
                    .setSize(1000)
                    .setScroll(new TimeValue(6000000))
                    .execute()
                    .actionGet();
            while (true) {
                for (SearchHit hit : sr.getHits()) {
                    SearchResponse sr1 = client.prepareSearch(ElasticConstant.PROFILES_INDEX)
                            .setTypes(ElasticConstant.PROFILES_TYPE)
                            .setQuery(QueryBuilders.boolQuery()
                                    .filter(QueryBuilders
                                            .matchPhraseQuery("attr.id", hit.id().split("_")[0])))
                            .execute()
                            .actionGet();
                    if (sr1.getHits().totalHits() > 0) {
                        SearchHit hit1 = sr1.getHits().getAt(0);
                        bulkInsert.addRequest(index, ElasticConstant.PROFILES_TYPE, hit1.id(), hit1.getSource());
                        if (bulkInsert.bulkSize() > bulkSize) {
                            BulkResponse response = bulkInsert.submitBulk();
                            logger.info(Thread.currentThread().getName() + " - Submit bulk took "
                                    + response.getTook().getSecondsFrac());
                        }
                    }
                }
                sr = client.prepareSearchScroll(sr.getScrollId())
                        .setScroll(new TimeValue(6000000)).execute().actionGet();
                if (sr.getHits().getHits().length == 0) break;
            }
        }
    }

    @Override
    public boolean checkConditionMust(Client client, String id) {
        SearchResponse rs = client.prepareSearch(ElasticConstant.INTERACT_INDEX)
                .setTypes(ElasticConstant.INTERACT_TYPE)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("fbpid", value)))
                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("guid", id))))
                .execute()
                .actionGet();

        if (rs.getHits().totalHits()>0) return true;
        return false;
    }
}
