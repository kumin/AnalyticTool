package com.vcc.bigdata.condition;

import com.vcc.bigdata.platform.elastic.ElasticBulkInsert;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @author: kumin on 07/07/2018
 **/

public class CounterCondition extends AdvanceCondition{
    @Override
    public QueryBuilder generateQuery() {
        return null;
    }


    @Override
    public void saveResultOr(ElasticBulkInsert bulkInsert, int bulkSize, String index) {

    }

    @Override
    public boolean checkConditionMust(Client client, String id) {
        return false;
    }
}
