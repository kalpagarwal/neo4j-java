package cl.fala.procedures.domainSpecifications.promotion.processRuleSet;

import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule.ProcessRuleInterface;
import cl.fala.procedures.Constants;
import cl.fala.procedures.domainSpecifications.getNodeFromRule.GetNodeFromRuleInterface;
import cl.fala.procedures.pojo.Operator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Node;

import java.util.*;

public class ProcessRuleSet implements ProcessRuleSetInterface {
    private final JSONObject ruleSet;
    private final ProcessRuleInterface processOrRule;
    private final ProcessRuleInterface processAndRule;
    private final GetNodeFromRuleInterface getNodeFromRuleInterface;

    public ProcessRuleSet(
            JSONObject ruleSet,
            GetNodeFromRuleInterface getNodeFromRuleInterface,
            ProcessRuleInterface processOrRule,
            ProcessRuleInterface processAndRule
    ) {
        this.ruleSet = ruleSet;
        this.getNodeFromRuleInterface = getNodeFromRuleInterface;
        this.processOrRule = processOrRule;
        this.processAndRule = processAndRule;
    }

    public Set<String> process() {
        return processInternal(this.ruleSet);
    }

    private boolean isRule(JSONObject rule) {
        return rule.has("label") && rule.has("value") && rule.has("operator");
    }

    private Set<String> applyOperator(
            List<Node> nodes,
            Operator operator,
            List<Set<String>> resolvedNodes) {
        if (operator.getOperator() == Operator.OPERATOR.AND) {
            return processAndRule.processRule(nodes, resolvedNodes);
        } else {
            return processOrRule.processRule(nodes, resolvedNodes);
        }
    }

    private Set<String> processInternal(JSONObject ruleSet) {
        if (ruleSet.has("operator")) {
            Operator operator = new Operator(ruleSet.getString("operator"));
            List<Node> nodes = new ArrayList<>();
            List<Set<String>> resolvedNodes = new ArrayList<>();
            if (ruleSet.has("rules")) {
                JSONArray rules = ruleSet.getJSONArray("rules");
                for (int i = 0; i < rules.length(); i++) {
                    JSONObject rule = rules.getJSONObject(i);
                    if (isRule(rule)) {
                        if(rule.getString("operator").equalsIgnoreCase("anyOf")) {
                            List<Node> nodeList = new ArrayList<Node>(getNodeFromRuleInterface.getNodes(rule));
                            resolvedNodes.add(applyOperator(nodeList, new Operator(Constants.OperatorConstant.anyOf), new ArrayList<>()));
                        } else {
                            Node entityNode = getNodeFromRuleInterface.getNode(rule);
                            List<Node> nodeList = new ArrayList<Node>(getNodeFromRuleInterface.getProductTypeNodes(entityNode));
                            if(nodeList.size()>0) {
                                resolvedNodes.add(applyOperator(nodeList, new Operator(Constants.OperatorConstant.anyOf), new ArrayList<>()));
                            } else {
                                nodes.add(entityNode);
                            }
                        }
                        
                    } else {
                        resolvedNodes.add(processInternal(rule));
                    }
                }
                return applyOperator(nodes, operator, resolvedNodes);
            }
        }

        return Collections.emptySet();
    }
}
