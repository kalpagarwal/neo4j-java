package cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule;

import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import cl.fala.procedures.utilities.StreamUtils;

import java.util.*;
import java.util.stream.Collectors;

public class ProcessAndRule implements ProcessRuleInterface {
    private final TraversalDescription traversalDescription;
    private final NodePropertyExtractorInterface nodePropertyExtractorInterface;

    public ProcessAndRule(TraversalDescription td, NodePropertyExtractorInterface nodePropertyExtractorInterface) {
        this.traversalDescription = td;
        this.nodePropertyExtractorInterface = nodePropertyExtractorInterface;
    }

    @Override
    public Set<String> processRule(List<Node> nodes, List<Set<String>> resolvedNodes) {
        Set<String> result = new HashSet<>();
        Boolean isFirst = true;

        for (Node node : nodes) {
            Set<String> offeringsIds = new HashSet<>();
            if(node != null) {
                Iterator<Path> pathIterator = traversalDescription.traverse(node).iterator();
                offeringsIds = StreamUtils
                    .asStream(pathIterator)
                    .map(Path::endNode)
                    .map(nodePropertyExtractorInterface::getProperty)
                    .collect(Collectors.toSet());
            }
            resolvedNodes.add(offeringsIds);
        }
        
        for (Set<String> resolvedNode : resolvedNodes) {
            if (isFirst) {
                isFirst = false;
                result.addAll(resolvedNode);
            } else {
                result.retainAll(resolvedNode);
            }
        }

        return result;
    }
}
