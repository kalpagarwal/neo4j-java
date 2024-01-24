package cl.fala.procedures;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public static class Promotion {
        public static final String CONDITIONS = "CONDITIONS";
        public static final String ACTIONS = "ACTIONS";
        public static final String ADD = "ADD";
        public static final String REMOVE = "REMOVE";
    }

    public static class GRAPH_LABELS {
        public static final String OFFERING = "Offering";
        public static final String PRODUCT_TYPE = "ProductType";
        public static final String VARIANT = "Variant";
        public static final String PRODUCT = "Product";
        public static final String CATEGORY = "Category";
    }

    public static class GRAPH_RELATIONSHIPS {
        public static final String HAS = "HAS";
        public static final String HAS_OFFERING = "HAS_OFFERING";

    }


    public static class CHILD_LABELS {
        public static List<String> Labels = Arrays.asList("Variant", "Product", "Offering");
    }

    public static class ALL_LABELS {
        public static List<String> Labels = Arrays.asList("Variant", "Product", "Offering", "Category", "Brand", "Collection", "Season", "Style");
    }

    public static class GRAPH_NODE_PROPERTIES {
        public static final String uuid = "uuid";
        public static final String tenant = "tenant";
        public static final String sites = "sites";


        public static class Offering {
            public static final String offeringId = "offeringId";
            public static final String sellerId = "sellerId";
            public static final String sellerName = "sellerName";
            public static final String uuid = "uuid";
            public static final String tenant = "tenant";
            public static final String isActive = "isActive";
        }
    }
    public static class OfferingProperties {
        public static List<String> Properties = Arrays.asList(Constants.GRAPH_NODE_PROPERTIES.Offering.uuid, Constants.GRAPH_NODE_PROPERTIES.Offering.offeringId, Constants.GRAPH_NODE_PROPERTIES.Offering.sellerId, Constants.GRAPH_NODE_PROPERTIES.Offering.sellerName, Constants.GRAPH_NODE_PROPERTIES.Offering.isActive);
        public static List<String> ReturnProperties = Arrays.asList(Constants.GRAPH_NODE_PROPERTIES.Offering.offeringId, Constants.GRAPH_NODE_PROPERTIES.Offering.sellerId, Constants.GRAPH_NODE_PROPERTIES.sites, Constants.GRAPH_NODE_PROPERTIES.Offering.sellerName, Constants.GRAPH_NODE_PROPERTIES.Offering.isActive);

    }

    public static class OperatorConstant {
        public static final String anyOf = "OR";
        public static final String allOff = "AND";
        public static final String OR = "OR";
        public static final String AND = "AND";
    }

    public static class GlobalOperator {
        public static final String anyOf = "anyOf";
        public static final String noneOf = "noneOf";
    }
    public static class GlobalObjectKeys {
        public static final String value = "value";
        public static final String operator = "operator";
        public static final String checkIsActiveFlag = "checkIsActiveFlag";
    }
}
