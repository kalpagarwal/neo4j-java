package cl.fala.procedures.promotion;

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
import cl.fala.procedures.pojo.PromotionSet;
import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import cl.fala.procedures.propertyExtractor.OfferingNodeOfferingIdExtractor;
import cl.fala.procedures.result.ProcessRuleSetResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ProcessSetProcedure {
    @Context
    public Transaction tx;

    @Procedure(value = "promodef.processSet", mode = Mode.READ)
    @Description("promodef.processSet({ruleset::String,exclusions::String,globalFilters::String}):: result :: String")
    public Stream<ProcessRuleSetResult> processSet(@Name("config") final Map<String, Object> config) throws InvalidPromotionException {
        List<ProcessRuleSetResult> emptyResult =  new ArrayList<>();
        try {
            String ruleSetParam = (String) config.get("ruleset");
            String exclusionsParam = (String) config.get("exclusions");
            String globalFiltersParam = (String) config.get("globalFilters");

            PromotionSet promotionSet = PromotionSet.createFromBase64(ruleSetParam, exclusionsParam, globalFiltersParam);
            TraversalDescription td = promotionSet.getTraversalDescription(tx, false);
            GetNodeFromRuleInterface getNodeFromRule = new GetNodeFromRule(tx);
            NodePropertyExtractorInterface nodePropertyExtractor = new OfferingNodeOfferingIdExtractor();
            ProcessRuleInterface processAndRule = new ProcessAndRule(td, nodePropertyExtractor);
            ProcessRuleInterface processOrRule = new ProcessOrRule(td, nodePropertyExtractor);
            ProcessRuleSetInterface processRuleSet = new ProcessRuleSet(promotionSet.getRuleSet().getRuleSet(), getNodeFromRule, processOrRule, processAndRule);
            ProcessRuleSetInterface processExclusions = new ProcessRuleSet(promotionSet.getExclusions().getRuleSet(), getNodeFromRule, processOrRule, processAndRule);
            ProcessSetInterface processSet = new ProcessSet(promotionSet, tx, processRuleSet, processExclusions);
            return processSet.process().map(ProcessRuleSetResult::new);
        } catch (NullPointerException nEx) {
            nEx.printStackTrace();
            return emptyResult.stream();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }
}
