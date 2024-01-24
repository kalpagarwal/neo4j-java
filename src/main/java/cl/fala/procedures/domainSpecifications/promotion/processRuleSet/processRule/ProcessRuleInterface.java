package cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Set;

public interface ProcessRuleInterface {
    Set<String> processRule(List<Node> nodes, List<Set<String>> resolvedNodesStack);
}
