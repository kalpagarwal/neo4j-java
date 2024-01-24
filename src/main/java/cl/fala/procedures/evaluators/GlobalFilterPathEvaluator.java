package cl.fala.procedures.evaluators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import cl.fala.procedures.Constants;
import cl.fala.procedures.pojo.GlobalFilter;

public class GlobalFilterPathEvaluator implements Evaluator {
    private final Label labelFilter;
    private final Map<String, Object> nodePropertyFilters;
    private GlobalFilter globalfilter;
    public GlobalFilterPathEvaluator(Label labelFilter, Map<String, Object> nodePropertyFilters, GlobalFilter globalFilter) {
        this.labelFilter = labelFilter;
        this.nodePropertyFilters = nodePropertyFilters;
        this.globalfilter = globalFilter;
    }
    private boolean propertyFilter(Node node) {
        boolean result = true;
        for (String key : this.nodePropertyFilters.keySet()) {
            Object resultKey = this.nodePropertyFilters.get(key);
            if (resultKey instanceof ArrayList) {
                ArrayList<String> arrayKey = (ArrayList<String>) resultKey;
                if (!arrayKey.isEmpty()) {
                    String nodeProperty = String.valueOf(node.getProperty(key, ""));
                    result = result && arrayKey.contains(nodeProperty);
                }
            } else {
                String filterValue = String.valueOf(this.nodePropertyFilters.get(key));
                result = result && String.valueOf(node.getProperty(key, "")).equals(filterValue);
            }
        }
        return result;
    }

    private boolean includeNode(Node node) {
        return node.hasLabel(labelFilter) && 
            globalfilter.filter(node) && 
            propertyFilter(node);
    }

    @Override
    public Evaluation evaluate(Path path) {
        if (path != null) {
            boolean nodeIsTarget = includeNode(path.endNode());
            return Evaluation.of(nodeIsTarget, !nodeIsTarget);
        } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

}
