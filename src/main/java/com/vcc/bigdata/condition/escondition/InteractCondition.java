package com.vcc.bigdata.condition.escondition;

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

import java.util.Collections;
import java.util.Date;
import java.util.Map;

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
        return QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("fbpid", value)))
                .must(QueryBuilders.boolQuery().filter(QueryBuilders
                        .termQuery("typeInter", getInteractSource(source) + "." + getInteractType(type))));
    }

    public String getInteractType(String type) {
        switch (type) {
            case LIKE:
                return "like";
            case POST:
                return "post";
            case COMMENT:
                return "cmt";
        }
        return null;
    }

    public String getInteractSource(String source) {
        switch (source) {
            case FANPAGE:
                return "fb.page";
            case GROUP:
                return "fb.group";
            case FORUM:
                return "forum";
        }
        return null;
    }

    @Override
    public void saveResultOr(ElasticBulkInsert bulkInsert, int bulkSize, String index, QueryBuilder profileTypeQuery) {
        Client client = bulkInsert.client();
        SearchResponse sr = client.prepareSearch(ElasticConstant.INTERACT_INDEX)
                .setTypes(ElasticConstant.INTERACT_TYPE)
                .setQuery(generateQuery())
                .setSize(1000)
                .setScroll(new TimeValue(6000000))
                .execute()
                .actionGet();
        while (true) {
            for (SearchHit hit : sr.getHits()) {
                SearchHit hit1 = getProfile(client, hit.getId(), profileTypeQuery);
                if (hit1 != null) {
                    Map<String, Object> source = hit1.getSource();
                    source.put("inserted_time",
                            Collections.singletonList(Collections.singletonMap("value",new Date())));
                    bulkInsert.addRequest(index, ElasticConstant.PROFILES_TYPE, hit1.id(), source);
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

    @Override
    public boolean checkConditionMust(Client client, String id) {
        SearchResponse rs = client.prepareSearch(ElasticConstant.INTERACT_INDEX)
                .setTypes(ElasticConstant.INTERACT_TYPE)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("fbpid", value)))
                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("typeInter",
                                getInteractSource(source) + "." + getInteractType(type))))
                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("guid", id))))
                .execute()
                .actionGet();

        return rs.getHits().totalHits() > 0;

    }

    @Override
    public SearchResponse getSatifyGuids(Client client) {
        return client.prepareSearch(ElasticConstant.INTERACT_INDEX)
                .setTypes(ElasticConstant.INTERACT_TYPE)
                .setQuery(generateQuery())
                .setScroll(new TimeValue(6000000))
                .setSize(1000)
                .execute()
                .actionGet();
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
