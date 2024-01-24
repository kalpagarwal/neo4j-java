package cl.fala.procedures.domainSpecifications.promotion;

import cl.fala.procedures.GetOfferings;
import cl.fala.procedures.domainSpecifications.promotion.processRuleSet.ProcessRuleSetInterface;
import cl.fala.procedures.pojo.PromotionSet;
import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import cl.fala.procedures.propertyExtractor.OfferingNodeOfferingIdExtractor;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcessSet implements ProcessSetInterface {
    private GetOfferings getOfferingsFromDb;
    private ProcessRuleSetInterface processRuleSet;
    private ProcessRuleSetInterface processExclusions;
    private PromotionSet promotionSet;
    private NodePropertyExtractorInterface offeringPropertyExtractor;

    public ProcessSet(
            PromotionSet promotionSet,
            Transaction tx,
            ProcessRuleSetInterface processRuleSet,
            ProcessRuleSetInterface processExclusions
    ) {
        getOfferingsFromDb = new GetOfferings(tx);
        this.processExclusions = processExclusions;
        this.processRuleSet = processRuleSet;
        this.promotionSet = promotionSet;
        this.offeringPropertyExtractor = new OfferingNodeOfferingIdExtractor();
    }


    @Override
    public Stream<String> process() {
        HashMap<String, String> propertyFilter = new HashMap<String, String>();
        propertyFilter.put("tenant", promotionSet.getGlobalFilter().getTenant());
        if (promotionSet.isGlobal()) {
            return Stream.empty();
        } else if (promotionSet.isExclusionOnly()) {
            Set<String> excludedItems = processExclusions.process();
             return getOfferingsFromDb.getAll(offeringPropertyExtractor, propertyFilter, promotionSet.getGlobalFilter())
                    .filter(offeringid -> !excludedItems.contains(offeringid));
        } else {
            Set<String> excludedItems = processExclusions.process();
            return processRuleSet.process().stream().filter(offeringId -> !excludedItems.contains(offeringId));
        }
    }
}
