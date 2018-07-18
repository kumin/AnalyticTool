package com.vcc.bigdata.condition.escondition;

import com.vcc.bigdata.platform.elastic.ElasticBulkInsert;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @author: kumin on 18/07/2018
 **/

public class ContentCondition extends AdvanceCondition{
    @Override
    public void saveResultOr(ElasticBulkInsert bulkInsert, int bulkSize, String index, QueryBuilder profileTypeQuery) {

    }

    @Override
    public boolean checkConditionMust(Client client, String id) {
        return false;
    }

    @Override
    public SearchResponse getSatifyGuids(Client client) {
        return null;
    }

    @Override
    public QueryBuilder generateQuery() {
        return null;
    }
}
