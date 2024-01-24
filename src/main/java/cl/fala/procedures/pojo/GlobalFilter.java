package cl.fala.procedures.pojo;

import cl.fala.procedures.Constants;
import cl.fala.procedures.Constants.GRAPH_NODE_PROPERTIES;
import cl.fala.procedures.Constants.GlobalObjectKeys;
import cl.fala.procedures.Constants.GlobalOperator;
import cl.fala.procedures.exceptions.InvalidPromotionException;
import cl.fala.procedures.exceptions.Messages;
import cl.fala.procedures.utilities.Parsing;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalFilter {
    private SellerId sellerId;
    private Sites sites;
    private SellerName sellerName;
    private ActiveFlag activeFlag;
    private String tenant;

    public GlobalFilter(SellerId sellerId, String tenant, Sites sites, SellerName sellerName, ActiveFlag activeFlag) {
        this.sellerId = sellerId;
        this.tenant = tenant;
        this.sites = sites;
        this.sellerName = sellerName;
        this.activeFlag = activeFlag;
    }

    public GlobalFilter(String filterAsBase64) throws InvalidPromotionException {
        this(new JSONObject(Parsing.decodeBase64(filterAsBase64)));
    }

    public GlobalFilter(JSONObject filterJson) throws InvalidPromotionException {
        this.sellerId = new SellerId(
            getValues(filterJson, GRAPH_NODE_PROPERTIES.Offering.sellerId),
            getOperator(filterJson, GRAPH_NODE_PROPERTIES.Offering.sellerId)
        );
        this.sellerName = new SellerName(
            getValues(filterJson, GRAPH_NODE_PROPERTIES.Offering.sellerName),
            getOperator(filterJson, GRAPH_NODE_PROPERTIES.Offering.sellerName)
        );
        this.sites = new Sites(
            getValues(filterJson, GRAPH_NODE_PROPERTIES.sites),
            getOperator(filterJson, GRAPH_NODE_PROPERTIES.sites)
        );
        if (filterJson.has(Constants.GlobalObjectKeys.checkIsActiveFlag)) {
            this.activeFlag = new ActiveFlag(
                filterJson.getBoolean(Constants.GlobalObjectKeys.checkIsActiveFlag)
            );
        } else {
            this.activeFlag = new ActiveFlag(true);
        }
        if (!filterJson.has(Constants.GRAPH_NODE_PROPERTIES.tenant)) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.TENANT_NOT_FOUND);
        }

        this.tenant = filterJson.getString(Constants.GRAPH_NODE_PROPERTIES.tenant);
    }

    private ArrayList<String> getValues(JSONObject filterJson, String fieldName) {
        ArrayList<String> values = new ArrayList<String>();
        if (filterJson.has(fieldName)) {
            JSONArray sellerValues = filterJson.optJSONArray(fieldName);
            if(sellerValues == null) {
                JSONObject seller = filterJson.getJSONObject(fieldName);
                sellerValues = seller.getJSONArray(GlobalObjectKeys.value);
            }
            
            for (int i = 0; i < sellerValues.length(); i++) {
                values.add(sellerValues.getString(i));
            }
        }
        return values;
    }
    private String getOperator(JSONObject filterJson, String fieldName) {
        String operator = GlobalOperator.anyOf;
        if(filterJson.has(fieldName)) {
            JSONArray sellerValues = filterJson.optJSONArray(fieldName);
            if(sellerValues == null) {
                JSONObject seller = filterJson.getJSONObject(fieldName);
                operator = seller.getString("operator");
            }
        }
        return operator;
    }
    public Map<String, Object> getAsMap() {
        Map<String, Object> filterMap = new HashMap<>();
        filterMap.put(Constants.GRAPH_NODE_PROPERTIES.Offering.sellerId, this.sellerId);
        filterMap.put(Constants.GRAPH_NODE_PROPERTIES.Offering.tenant, this.tenant);
        filterMap.put(Constants.GRAPH_NODE_PROPERTIES.sites, this.sites);
        filterMap.put(Constants.GRAPH_NODE_PROPERTIES.Offering.sellerName, this.sellerName);
        filterMap.put(Constants.GlobalObjectKeys.checkIsActiveFlag, this.activeFlag);

        return filterMap;
    }

    public Seller getSellerId() {
        return this.sellerId;
    }

    public String getTenant() {
        return this.tenant;
    }
    public Sites getSite() {
        return this.sites;
    }
    public Seller getSellerName() {
        return this.sellerName;
    }

    public ActiveFlag getActiveflag() {
        return this.activeFlag;
    }

    public boolean isGlobal() {
        return (sellerId == null || sellerId.isEmpty()) && (sites == null || sites.isEmpty()) && (sellerName == null || sellerName.isEmpty());
    }

    public String getAsBase64() {
        return Parsing.encodeToBase64(this.toJSON().toString());
    }

    public JSONObject toJSON() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sellerId", this.sellerId.toJSON());
        jsonObject.put("tenant", this.tenant);
        jsonObject.put("sites", new JSONArray(this.sites));
        jsonObject.put("sellerName", this.sellerName.toJSON());
        jsonObject.put("activeFlag", this.activeFlag.toJSON());

        return jsonObject;
    }
    public boolean filter(Node offering) {
        return this.sellerId.filter(offering) &&
            this.sellerName.filter(offering) &&
            this.activeFlag.filter(offering) &&
            this.sites.filter(offering);
    }
}
