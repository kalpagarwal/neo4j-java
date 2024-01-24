package cl.fala.procedures.evaluators;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import cl.fala.procedures.Constants;

public class PathEvaluatorNodeFilters implements Evaluator {
    private final List<Node> nodeFilter;
    private final boolean productTypeFilter;


    public PathEvaluatorNodeFilters(List<Node> nodeFilter, boolean productTypeFilter) {
        this.nodeFilter = nodeFilter;
        this.productTypeFilter = productTypeFilter;
    }

    private boolean includeNode(Node node) {
        boolean filterMatch = false;
        for(Node toNode: this.nodeFilter) {
            if(toNode.equals(node)) {
                filterMatch = true;
            }
        }
        if(!filterMatch && this.productTypeFilter) {
            filterMatch = node.hasLabel(Label.label(Constants.GRAPH_LABELS.VARIANT));
        }
        return filterMatch;
    }

    @Override
    public Evaluation evaluate(Path path) {
        if (path != null) {
            if(includeNode(path.endNode()))
                return Evaluation.INCLUDE_AND_CONTINUE;
            else 
                return Evaluation.EXCLUDE_AND_CONTINUE;
        } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

}
