package cl.fala.procedures.evaluators;

import java.util.Iterator;
import java.util.List;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class PathEvaluatorByExcludingNode implements Evaluator {
    private final List<Node> excludeNodes;
    private final Node fromNode;
    private final boolean excludeFromNode;
    public PathEvaluatorByExcludingNode(List<Node> excludeNodes, Node fromNode, boolean excludeFromNode) {
        this.excludeNodes = excludeNodes;
        this.fromNode = fromNode;
        this.excludeFromNode = excludeFromNode;
    }
    private boolean isExcludedNode(Node endNode, Node otherNode) {
        return (
            (this.excludeNodes.contains(endNode) && fromNode.equals(otherNode)) ||
            (this.excludeNodes.contains(otherNode) && fromNode.equals(endNode))
        ) || (excludeFromNode && (fromNode.equals(endNode)));
    }
    private boolean includeNode(Path path) {
        Node endNode = path.endNode();
        Node otherNode = null;
        Iterator<Relationship> pathItr = path.reverseRelationships().iterator();
        if(pathItr.hasNext()) {
            Relationship lastRelationship = path.reverseRelationships().iterator().next();
            otherNode = lastRelationship.getOtherNode(endNode);
        }
        return !isExcludedNode(endNode, otherNode);
    }

    @Override
    public Evaluation evaluate(Path path) {
        if (path != null) {
            if(includeNode(path))
                return Evaluation.INCLUDE_AND_CONTINUE;
            else 
                return Evaluation.EXCLUDE_AND_PRUNE;
        } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
    }
}
