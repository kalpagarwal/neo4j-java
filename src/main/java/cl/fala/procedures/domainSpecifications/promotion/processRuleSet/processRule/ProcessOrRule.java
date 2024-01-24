package cl.fala.procedures.domainSpecifications.promotion.processRuleSet.processRule;

import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import cl.fala.procedures.utilities.StreamUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProcessOrRule implements ProcessRuleInterface {
    private final TraversalDescription traversalDescription;
    private final NodePropertyExtractorInterface nodePropertyExtractorInterface;

    public ProcessOrRule(TraversalDescription td, NodePropertyExtractorInterface nodePropertyExtractorInterface) {
        this.traversalDescription = td;
        this.nodePropertyExtractorInterface = nodePropertyExtractorInterface;
    }

    public Set<String> processRule(List<Node> nodes, List<Set<String>> resolvedNodes) {
        nodes = nodes.stream().filter(node-> node!=null).collect(Collectors.toList());
        Iterator<Path> pathIterator = traversalDescription.traverse(nodes).iterator();
        Set<String> result = StreamUtils
                .asStream(pathIterator)
                .map(Path::endNode)
                .map(nodePropertyExtractorInterface::getProperty)
                .collect(Collectors.toSet());
        for (Set<String> resolvedNode : resolvedNodes) {
            result.addAll(resolvedNode);
        }

        return result;
    }
}
