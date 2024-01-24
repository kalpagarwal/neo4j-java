package cl.fala.procedures.pojo;

import cl.fala.procedures.Constants;
import cl.fala.procedures.evaluators.GlobalFilterPathEvaluator;
import cl.fala.procedures.evaluators.PathEvaluator;
import cl.fala.procedures.exceptions.InvalidPromotionException;
import cl.fala.procedures.utilities.Parsing;

import java.util.HashMap;

import org.json.JSONObject;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class PromotionSet {
    private RuleSet ruleSet;
    private RuleSet exclusions;
    private GlobalFilter globalFilter;

    public PromotionSet(RuleSet ruleSet, RuleSet exclusions, GlobalFilter globalFilter) {
        this.ruleSet = ruleSet;
        this.exclusions = exclusions;
        this.globalFilter = globalFilter;
    }

    public static PromotionSet createFromJSON(JSONObject setJson, JSONObject filterJSON) throws InvalidPromotionException {
        RuleSet ruleSet = new RuleSet();
        RuleSet exclusions = new RuleSet();
        if (setJson.has("ruleset")) {
            ruleSet = new RuleSet(setJson.getJSONObject("ruleset"));
        }
        if (setJson.has("exclusions")) {
            exclusions = new RuleSet(setJson.getJSONObject("exclusions"));
        }
        GlobalFilter globalFilter = new GlobalFilter(filterJSON);
        return new PromotionSet(ruleSet, exclusions, globalFilter);
    }

    public static PromotionSet createFromBase64(String ruleSetAsBase64, String exclusionsAsBase64, String globalFiltersBase64) throws InvalidPromotionException {
        RuleSet ruleSet = new RuleSet(new JSONObject(Parsing.decodeBase64(ruleSetAsBase64)));
        RuleSet exclusions = new RuleSet(new JSONObject(Parsing.decodeBase64(exclusionsAsBase64)));
        GlobalFilter globalFilter = new GlobalFilter(new JSONObject(Parsing.decodeBase64(globalFiltersBase64)));
        return new PromotionSet(ruleSet, exclusions, globalFilter);
    }

    public GlobalFilter getGlobalFilter() {
        return globalFilter;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    public RuleSet getExclusions() {
        return exclusions;
    }

    public boolean isGlobal() {
        return this.ruleSet.getRuleSet().isEmpty() && this.exclusions.getRuleSet().isEmpty();
    }

    public boolean isFilterOnly(GlobalFilter filter) {
        return this.ruleSet.getRuleSet().isEmpty() && this.exclusions.getRuleSet().isEmpty() && !filter.isGlobal();
    }

    public boolean isExclusionOnly() {
        return this.ruleSet.getRuleSet().isEmpty() && !this.exclusions.getRuleSet().isEmpty();
    }

    public TraversalDescription getTraversalDescription(Transaction tx, boolean bfsTraversing) {
        RelationshipType relationshipType = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        HashMap<String, Object> proertyFilter = new HashMap<String, Object>();
        Evaluator evaluator = new GlobalFilterPathEvaluator('OFFERING', proertyFilter, globalFilter);
        TraversalDescription td = tx.traversalDescription();

        td = bfsTraversing ? td.breadthFirst(): td.depthFirst();
        return td.relationships(relationshipType, Direction.OUTGOING)
            .evaluator(evaluator);
    }
}
