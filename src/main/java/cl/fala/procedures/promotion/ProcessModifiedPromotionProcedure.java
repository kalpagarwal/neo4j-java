package cl.fala.procedures.promotion;

import cl.fala.procedures.Constants;
import cl.fala.procedures.domainSpecifications.getNodeFromRule.GetNodeFromRule;
import cl.fala.procedures.domainSpecifications.getNodeFromRule.GetNodeFromRuleInterface;
import cl.fala.procedures.domainSpecifications.promotion.ProcessSet;
import cl.fala.procedures.domainSpecifications.promotion.ProcessSetInterface;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.ProcessRuleSet;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.ProcessRuleSetInterface;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule.ProcessAndRule;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule.ProcessOrRule;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule.ProcessRuleInterface;
import cl.fala.procedures.exceptions.InvalidPromotionException;
import cl.fala.procedures.pojo.Promotion;
import cl.fala.procedures.pojo.PromotionSet;
import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import cl.fala.procedures.propertyExtractor.OfferingNodeOfferingIdExtractor;
import cl.fala.procedures.result.ProcessModifiedPromotionResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcessModifiedPromotionProcedure {
    @Context
    public Transaction tx;

    private Stream<String> getOfferingsForPromotionSet(PromotionSet promotionSet) {
        TraversalDescription td = promotionSet.getTraversalDescription(tx, false);
        GetNodeFromRuleInterface getNodeFromRule = new GetNodeFromRule(tx);
        NodePropertyExtractorInterface nodePropertyExtractor = new OfferingNodeOfferingIdExtractor();
        ProcessRuleInterface processAndRule = new ProcessAndRule(td, nodePropertyExtractor);
        ProcessRuleInterface processOrRule = new ProcessOrRule(td, nodePropertyExtractor);
        ProcessRuleSetInterface processRuleSet = new ProcessRuleSet(promotionSet.getRuleSet().getRuleSet(), getNodeFromRule, processOrRule, processAndRule);
        ProcessRuleSetInterface processExclusions = new ProcessRuleSet(promotionSet.getExclusions().getRuleSet(), getNodeFromRule, processOrRule, processAndRule);
        ProcessSetInterface processSet = new ProcessSet(promotionSet, tx, processRuleSet, processExclusions);
        return processSet.process();
    }

    private Stream<ProcessModifiedPromotionResult> getOfferingsForModifiedPromotion(Promotion oldVersion, Promotion newVersion) {
        Set<String> newPromotionConditionsOfferings = newVersion.shouldProcessConditions() ? getOfferingsForPromotionSet(newVersion.getConditions()).collect(Collectors.toSet()) : new HashSet<>();
        Set<String> newPromotionActionsOfferings = newVersion.shouldProcessActions() ? getOfferingsForPromotionSet(newVersion.getActions()).collect(Collectors.toSet()) : new HashSet<>();
        Set<String> offeringsDiffInConditions = oldVersion.shouldProcessConditions() ? getOfferingsForPromotionSet(oldVersion.getConditions()).collect(Collectors.toSet()) : new HashSet<>();
        Set<String> offeringsDiffInActions =  oldVersion.shouldProcessActions() ? getOfferingsForPromotionSet(oldVersion.getActions()).collect(Collectors.toSet()) : new HashSet<>();
        Stream<ProcessModifiedPromotionResult> offeringsDiffInConditionsResultStream,
                offeringsDiffInActionsResultStream,
                promotionDiffOfferingsResultStream,
                newOfferingsConditionsResultStream,
                newOfferingsActionsResultStream,
                newPromotionOfferingsResultStream;

        offeringsDiffInConditions.removeAll(newPromotionConditionsOfferings);
        offeringsDiffInActions.removeAll(newPromotionActionsOfferings);

        offeringsDiffInConditionsResultStream = offeringsDiffInConditions
                .stream()
                .map(offeringId -> new ProcessModifiedPromotionResult(offeringId, Constants.Promotion.CONDITIONS, Constants.Promotion.REMOVE));
        offeringsDiffInActionsResultStream = offeringsDiffInActions
                .stream()
                .map(offeringId -> new ProcessModifiedPromotionResult(offeringId, Constants.Promotion.ACTIONS, Constants.Promotion.REMOVE));
        promotionDiffOfferingsResultStream = Stream.concat(offeringsDiffInConditionsResultStream, offeringsDiffInActionsResultStream);

        newOfferingsConditionsResultStream = newPromotionConditionsOfferings.stream().map(offeringId -> new ProcessModifiedPromotionResult(offeringId, Constants.Promotion.CONDITIONS, Constants.Promotion.ADD));
        newOfferingsActionsResultStream = newPromotionActionsOfferings.stream().map(offeringId -> new ProcessModifiedPromotionResult(offeringId, Constants.Promotion.ACTIONS, Constants.Promotion.ADD));
        newPromotionOfferingsResultStream = Stream.concat(newOfferingsConditionsResultStream, newOfferingsActionsResultStream);

        return Stream.concat(promotionDiffOfferingsResultStream, newPromotionOfferingsResultStream);
    }

    @Procedure(value = "promodef.getOfferingsForModifiedPromotion", mode = Mode.READ)
    @Description("promodef.getOfferingsForModifiedPromotion({oldPromotion::String,newPromotion::String}):: result :: String")
    public Stream<ProcessModifiedPromotionResult> getOfferingsForModifiedPromotion(@Name("config") final Map<String, Object> config) throws InvalidPromotionException {
        List<ProcessModifiedPromotionResult> emptyResult = new ArrayList<>();
        try {
            String oldPromotionAsString = (String) config.get("oldPromotion");
            String newPromotionAsString = (String) config.get("newPromotion");

            Promotion oldPromotion = Promotion.createFromBase64(oldPromotionAsString);
            Promotion newPromotion = Promotion.createFromBase64(newPromotionAsString);

            return getOfferingsForModifiedPromotion(oldPromotion, newPromotion);
        } catch (NullPointerException nEx) {
            nEx.printStackTrace();
            return emptyResult.stream();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }
}
