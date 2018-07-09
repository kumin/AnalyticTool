package com.vcc.bigdata.campaign;

import java.util.List;
import java.util.concurrent.Future;

/**
 * @author: kumin on 02/07/2018
 **/

public interface CampaignRepo {

    Future updateCampaign(String id, Campaign campaign);
    List<Campaign> getCampaigns();
    Future delCampaign(String id);
    Future addCampaign(Campaign campaign);
}
