package com.vcc.bigdata.campaign;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vcc.bigdata.common.config.Properties;
import com.vcc.bigdata.common.types.IdGenerator;
import com.vcc.bigdata.common.types.RandomIdGenerator;
import com.vcc.bigdata.common.utils.Utils;
import com.vcc.bigdata.platform.elastic.ElasticClientProvider;
import com.vcc.bigdata.platform.elastic.ElasticConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author: kumin on 02/07/2018
 **/

public class ElasticCampaignRepo implements CampaignRepo {
    private static final String ES_INDEX = "datacollection-campaigns";
    private static final String ES_TYPE = "campaign";
    private static Client client;
    private IdGenerator idGenerator;


    public ElasticCampaignRepo(Properties props) {
        ElasticConfig config = new ElasticConfig(props);
        client = ElasticClientProvider.getOrCreate("analytic", config);
        idGenerator = new RandomIdGenerator();
    }


    @Override
    public Future updateCampaign(String id, Campaign campaign) {
        UpdateRequest updateRequest = new UpdateRequest(ES_INDEX, ES_TYPE, id)
                .doc(getJsonSource(campaign));

        return client.update(updateRequest);
    }

    @Override
    public List<Campaign> getCampaigns() {
        List<Campaign> campaigns = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();
        SearchResponse sr = client.prepareSearch(ES_INDEX)
                .setTypes("campaign")
                .setQuery(QueryBuilders.boolQuery()
                                .should(QueryBuilders.termQuery("status", Campaign.NON_PROCESS))
                                .should(QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery("status", Campaign.PROCESSING))
                                        .must(QueryBuilders.termQuery("host.keyword", Utils.getHostName()))))
                .addSort("created_date", SortOrder.ASC)
                .setSize(100)
                .execute()
                .actionGet();

        for (SearchHit hit : sr.getHits()){
            try {
                Campaign campaign = om.readValue(hit.sourceAsString(), Campaign.class);
                campaign.setId(hit.id());
                campaigns.add(campaign);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return campaigns;
    }

    @Override
    public Future delCampaign(String id) {
        return null;
    }

    @Override
    public Future<IndexResponse> addCampaign(Campaign campaign) {
        long camId = idGenerator.generate();
        IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_TYPE, String.valueOf(camId))
                .source(getJsonSource(campaign));

        return client.index(indexRequest);
    }

    public String getJsonSource(Campaign campaign) {
        ObjectMapper om = new ObjectMapper();
        try {
            return om.writeValueAsString(campaign);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}

