package cl.fala.procedures.pojo;

import cl.fala.procedures.Constants;
import cl.fala.procedures.exceptions.InvalidPromotionException;
import cl.fala.procedures.exceptions.Messages;
import cl.fala.procedures.utilities.Parsing;

import java.util.ArrayList;
import org.json.JSONObject;

public class Promotion {
    private PromotionSet conditions;
    private PromotionSet actions;
    private String tenant;
    private String id;
    private boolean shouldProcessActions = true;
    private boolean shouldProcessConditions = true;


    public Promotion(PromotionSet conditions, PromotionSet actions, String tenant, String id, boolean shouldProcessActions, boolean shouldProcessConditions) {
        this.conditions = conditions;
        this.actions = actions;
        this.tenant = tenant;
        this.id = id;
        this.shouldProcessActions = shouldProcessActions;
        this.shouldProcessConditions = shouldProcessConditions;
    }

    public static Promotion createFromJSON(JSONObject promotionJson) throws InvalidPromotionException {
        if (!promotionJson.has("id")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.ID_NOT_FOUND);
        }
        if (!promotionJson.has("tenant")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.TENANT_NOT_FOUND);
        }
        if (!promotionJson.has("sellerId")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.SELLER_ID_NOT_FOUND);
        }
        if (!promotionJson.has("sites")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.SITES_NOT_FOUND);
        }
        if (!promotionJson.has("sellerName")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.SELLER_NAME_NOT_FOUND);
        }
        if (!promotionJson.has("conditions")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.MISSING_CONDITIONS);
        }
        if (!promotionJson.has("actions")) {
            throw new InvalidPromotionException(Messages.PromotionExceptions.MISSING_ACTIONS);
        }

        String tenant = promotionJson.getString("tenant");

        JSONObject globalFilters = new JSONObject();
        globalFilters.put("tenant", tenant);
        globalFilters.put("sellerId", promotionJson.getJSONObject("sellerId"));
        globalFilters.put("sites", promotionJson.getJSONObject("sites"));
        globalFilters.put("sellerName", promotionJson.getJSONObject("sellerName"));

        if(promotionJson.has(Constants.GlobalObjectKeys.checkIsActiveFlag)) {
            globalFilters.put(
              Constants.GlobalObjectKeys.checkIsActiveFlag,
              promotionJson.getBoolean(Constants.GlobalObjectKeys.checkIsActiveFlag)
            );
        }

        PromotionSet conditions = PromotionSet.createFromJSON(promotionJson.getJSONObject("conditions"), globalFilters);
        PromotionSet actions = PromotionSet.createFromJSON(promotionJson.getJSONObject("actions"), globalFilters);

        String id = promotionJson.getString("id");

        boolean shouldProcessActions = true;
        if (promotionJson.has("shouldProcessActions")) {
            shouldProcessActions = promotionJson.getBoolean("shouldProcessActions");
        }
        boolean shouldProcessConditions = true;
        if (promotionJson.has("shouldProcessConditions")) {
            shouldProcessConditions = promotionJson.getBoolean("shouldProcessConditions");
        }

        return new Promotion(conditions, actions, tenant, id, shouldProcessActions, shouldProcessConditions);
    }

    public static Promotion createFromBase64(String promotionAsBase64) throws InvalidPromotionException {
        return Promotion.createFromJSON(new JSONObject(Parsing.decodeBase64(promotionAsBase64)));
    }

    public String getId() {
        return id;
    }

    public String getTenant() {
        return tenant;
    }

    public PromotionSet getConditions() {
        return conditions;
    }

    public PromotionSet getActions() {
        return actions;
    }

    public boolean shouldProcessActions() {
        return shouldProcessActions;
    }
    public boolean shouldProcessConditions() {
        return shouldProcessConditions;
    }
}
