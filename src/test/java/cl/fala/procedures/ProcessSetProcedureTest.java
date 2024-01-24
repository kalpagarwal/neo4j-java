package cl.fala.procedures;

import cl.fala.procedures.promotion.ProcessSetProcedure;
import cl.fala.procedures.utilities.Parsing;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProcessSetProcedureTest {
    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Driver neo4jDriver;

    @BeforeAll
    void initializeNeo4j() {
        Neo4j neo4j = Neo4jBuilders
                .newInProcessBuilder()
                .withProcedure(ProcessSetProcedure.class)
                .build();
        this.neo4jDriver = GraphDatabase.driver(neo4j.boltURI(), driverConfig);
        setUpGraph();
    }

    /*
    Creates Graph:
    p1  p2
    | \ | \
    o1 o2  o3

    p1 p2
    |  |
    v1  v2
    | \ | \
    o1 o2 o3
    s1
    | \
    o1 o3
     */
    private void setUpGraph() {
        try (Session session = neo4jDriver.session();
             Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (v1:Variant{uuid:'v1',tenant:'FACL', productType:'pt1'})");
                tx.run("CREATE (v2:Variant{uuid:'v2',tenant:'FACL'})");
                tx.run("CREATE (p1:Product{uuid:'p1',tenant:'FACL'})");
                tx.run("CREATE (p2:Product{uuid:'p2',tenant:'FACL'})");
                tx.run("CREATE (s1:Style{uuid:'s1',tenant:'FACL'})");
                tx.run("CREATE (pt1:ProductType{uuid:'pt1',tenant:'FACL', productType:'pt1'})");
                tx.run("CREATE (o1:Offering:FACL{uuid:'o1',offeringId:'o1',sellerId:'FALABELLA',tenant:'FACL', sellerName:'FALABELLA', isActive: true})");
                tx.run("CREATE (o2:Offering:FACL{uuid:'o2',offeringId:'o2',sellerId:'FALABELLA',tenant:'FACL', sellerName:'FALABELLA', isActive: true})");
                tx.run("CREATE (o3:Offering:SOCL{uuid:'o3',offeringId:'o3',sellerId:'SAMPLE',tenant:'FACL', sellerName:'SAMPLE'})");
                tx.run("CREATE (o4:Offering:SOCL{uuid:'o4',offeringId:'o4',sellerId:'FALABELLA',tenant:'FACL', sellerName:'FALABELLA', isActive: false})");


            String createRelationshipsQuery = "MATCH (p1:Product{uuid:'p1'}) " +
                    "MATCH (p2:Product{uuid:'p2'}) " +
                    "MATCH (o1:Offering{uuid:'o1'}) " +
                    "MATCH (o2:Offering{uuid:'o2'}) " +
                    "MATCH (o3:Offering{uuid:'o3'}) " +
                    "MATCH (o4:Offering{uuid:'o4'}) " +
                    "MATCH (v1:Variant{uuid:'v1'}) " +
                    "MATCH (v2:Variant{uuid:'v2'}) " +
                    "MATCH (s1:Style{uuid:'s1'}) " +
                    "MATCH (pt1:ProductType{uuid:'pt1'}) " +
                    "MERGE (p1)-[:HAS]->(v1) " +
                    "MERGE (p2)-[:HAS]->(v2) " +
                    "MERGE (v1)-[:HAS]->(o1) " +
                    "MERGE (v1)-[:HAS]->(o2) " +
                    "MERGE (v2)-[:HAS]->(o2) " +
                    "MERGE (v1)-[:HAS]->(o4) " +
                    "MERGE (v2)-[:HAS]->(o3) "+
                    "MERGE (s1)-[:HAS]->(o1) " +
                    "MERGE (s2)-[:HAS]->(o3) ";

            tx.run(createRelationshipsQuery);

            tx.commit();
        }
    }

    @AfterAll
    void tearDown() {
        this.neo4jDriver.close();
    }

    @Test
    public void shouldThrowIfTenantIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {
            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64("{}"));

            session.run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
    }

    @Test
    void shouldReturnEmptyDataIfRuleSetIsInvalid() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");

            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyOfferingsIfRuleSetAndExclusionsEmpty() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId as applicableEntityIds RETURN applicableEntityIds", params);
            Stream<Record> records = result.stream();
            assertThat(records.count()).isEqualTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyOfferingsIfRuleSetAndExclusionsEmptyAndGlobalFilterContainsTenantOnly() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("sellerId", new JSONArray());
            globalFilters.put("tenant", "FACL");
            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId as applicableEntityIds RETURN applicableEntityIds", params);
            Stream<Record> records = result.stream();
            assertThat(records.count()).isEqualTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldReturnAllOfferingIfRuleSetAndExclusionsEmptyAndGlobalFilterContainsSitesOnly() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONArray sellerName = new JSONArray();
            JSONArray sellerId = new JSONArray();

            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            sites.put(0, "FACL");
            globalFilters.put("sellerId", sellerId);
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldReturnAllOfferingIfRuleSetAndExclusionsEmptyAndGlobalFilterContainsSellerNameOnly() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONArray sellerName = new JSONArray();
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);
            sellerName.put(0, "FALABELLA");
            globalFilters.put("sellerId", new JSONArray());
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldReturnCorrectApplicableIdsForNonEmptyRuleSetAndEmptyExclusionsForOROperator() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            resultRecords.add("o3");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnCorrectApplicableIdsForNonEmptyRuleSetAndNonEmptyExclusionsForOROperator() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue: 'p1'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue: 'p2'}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject exclusions = new JSONObject();
            exclusions.put("operator", "OR");
            JSONArray exclusionRules = new JSONArray();
            JSONObject exclusionRule1 = new JSONObject();
            exclusionRule1.put("value", new JSONObject("{singleValue: 'p1'}"));
            exclusionRule1.put("operator", "isEqualTo");
            exclusionRule1.put("label", "Product");
            exclusionRules.put(0, exclusionRule1);
            exclusions.put("rules", exclusionRules);
            JSONArray sites = new JSONArray();
            sites.put(0, "FACL");
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sites", sites);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnCorrectApplicableIdsForNonEmptyRuleSetOROperatorIfOneOfAttributeNotExist() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue: 'p1'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue: 'p9'}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject exclusions = new JSONObject();
            JSONArray sites = new JSONArray();
            sites.put(0, "FACL");
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sites", sites);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyApplicableIdsForNonEmptyRuleSetANDOperatorIfOneOfAttributeNotExist() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue: 'p9'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue: 'p1'}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject exclusions = new JSONObject();
            JSONArray sites = new JSONArray();
            sites.put(0, "FACL");
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sites", sites);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyApplicableIdsForNonEmptyRuleSetANDOperatorIfOneAttributeNotExistInNeo4j() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue: 'p1'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue: 'p9'}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject exclusions = new JSONObject();
            JSONArray sites = new JSONArray();
            sites.put(0, "FACL");
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sites", sites);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnCorrectApplicableIdsForNonEmptyRuleSetAndEmptyExclusionsForANDOperator() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue: 'p1'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue: 'p2'}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "FACL");
            JSONArray sites = new JSONArray();

            sites.put(0, "SOCL");
            globalFilters.put("sites", sites);
            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnCorrectApplicableIdsForCombinationOfRules() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            JSONObject rule2 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue:'p2'}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");

            rule2.put("value", new JSONObject("{multiValue:['p2', 'p1']}"));
            rule2.put("operator", "anyOf");
            rule2.put("label", "Product");
            rules.put(0, rule1);
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            sellerId.add("FALABELLA");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o2");


            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldApplySellerIdFilter() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue:p2}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            sellerId.add("FALABELLA");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o2");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnProperOfferingsForExclusionsOnlyAndSellerAll() {
        try (Session session = neo4jDriver.session()) {
            JSONObject exclusions = new JSONObject();
            exclusions.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue:o2}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Offering");
            rules.put(0, rule1);
            exclusions.put("rules", rules);

            JSONArray sites = new JSONArray();
            sites.put(0, "FACL");
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("sellerId", new JSONArray());
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sites", sites);


            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnProperOfferingsForExclusionsOnly() {
        try (Session session = neo4jDriver.session()) {
            JSONObject exclusions = new JSONObject();
            exclusions.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{singleValue:o2}"));
            rule1.put("operator", "isEqualTo");
            rule1.put("label", "Offering");
            rules.put(0, rule1);
            exclusions.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            sellerId.add("FALABELLA");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64(exclusions.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldNotReturnOfferingsForGlobalPromotion() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            ArrayList<String> sites = new ArrayList<>();
            sellerId.add("FALABELLA");
            sites.add("FACL");
            sites.add("SOCL");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("sites", new JSONArray(sites));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyOfferingsForGlobalSet() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            globalFilters.put("tenant", "SOCL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64("{}"));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records.size()).isEqualTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnProperOfferingsForCombinationOfRulesWithAnyEmptyRulesUsingANDOperator() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue:p2}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            sellerId.add("FALABELLA");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records.size()).isEqualTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnProperOfferingsForCombinationOfRulesWithAnyEmptyRulesUsingOROperator() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rules.put(0, rule1);
            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{singleValue:p2}"));
            rule2.put("operator", "isEqualTo");
            rule2.put("label", "Product");
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            ArrayList<String> sellerName = new ArrayList<>();
            sellerId.add("FALABELLA");
            sellerName.add("FALABELLA");
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("sellerName", new JSONArray(sellerName));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o2");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnCorrectApplicableIdsForMultiLevelRules() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONArray interNalRules = new JSONArray();

            JSONObject rule1 = new JSONObject();
            rule1.put("operator", "OR");
            JSONObject rule11 = new JSONObject();
            JSONObject rule12 = new JSONObject();

            rule11.put("value", new JSONObject("{singleValue:'o1'}"));
            rule11.put("operator", "isEqualTo");
            rule11.put("label", "Offering");
            rule12.put("value", new JSONObject("{singleValue:'o3'}"));
            rule12.put("operator", "isEqualTo");
            rule12.put("label", "Offering");

            interNalRules.put(0, rule11);
            interNalRules.put(1, rule12);
            rule1.put("rules", interNalRules);

            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{multiValue:['p2', 'p1']}"));
            rule2.put("operator", "anyOf");
            rule2.put("label", "Product");

            rules.put(0, rule1);
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o3");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldReturnCorrectApplicableIdsForMultiLevelRulesForStyle() {
        try (Session session = neo4jDriver.session()) {
            JSONObject ruleSet = new JSONObject();
            ruleSet.put("operator", "AND");
            JSONArray rules = new JSONArray();
            JSONArray interNalRules = new JSONArray();

            JSONObject rule1 = new JSONObject();
            rule1.put("operator", "AND");
            JSONObject rule11 = new JSONObject();
            JSONObject rule12 = new JSONObject();

            rule11.put("value", new JSONObject("{singleValue:'s1'}"));
            rule11.put("operator", "isEqualTo");
            rule11.put("label", "Style");
            rule12.put("value", new JSONObject("{singleValue:'p1'}"));
            rule12.put("operator", "isEqualTo");
            rule12.put("label", "Product");

            interNalRules.put(0, rule11);
            interNalRules.put(1, rule12);
            rule1.put("rules", interNalRules);

            JSONObject rule2 = new JSONObject();
            rule2.put("value", new JSONObject("{multiValue:['pt1']}"));
            rule2.put("operator", "anyOf");
            rule2.put("label", "ProductType");

            rules.put(0, rule1);
            rules.put(1, rule2);
            ruleSet.put("rules", rules);

            JSONObject globalFilters = new JSONObject();
            ArrayList<String> sellerId = new ArrayList<>();
            globalFilters.put("sellerId", new JSONArray(sellerId));
            globalFilters.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldReturnOfferingWithGlobalFilter() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONObject sellerName = new JSONObject();
            JSONObject sellerId = new JSONObject();
            JSONObject ruleSet = new JSONObject();
            JSONArray sellerValue = new JSONArray();
            sellerValue.put(0, "FALABELLA");

            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            sellerName.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerName.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.anyOf);

            sellerId.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerId.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.anyOf);

            globalFilters.put("sellerId", sellerId);
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldReturnOfferingWithGlobalFilterNoneOf() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONObject sellerName = new JSONObject();
            JSONObject sellerId = new JSONObject();
            JSONObject ruleSet = new JSONObject();
            JSONArray sellerValue = new JSONArray();
            sellerValue.put(0, "FALABELLA");
            
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            sellerName.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerName.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            sellerId.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerId.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            globalFilters.put("sellerId", sellerId);
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);

            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o3");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldReturnOfferingWithGlobalFilter_checkIsActiveFlag_false() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONObject sellerName = new JSONObject();
            JSONObject sellerId = new JSONObject();
            JSONObject ruleSet = new JSONObject();
            JSONArray sellerValue = new JSONArray();
            sellerValue.put(0, "SAMPLE");
            
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            sellerName.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerName.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            sellerId.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerId.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            globalFilters.put("sellerId", sellerId);
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);
            globalFilters.put("checkIsActiveFlag", false);


            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            resultRecords.add("o4");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldReturnOfferingWithGlobalFilter_checkIsActiveFlag_true() {
        try (Session session = neo4jDriver.session()) {
            JSONObject globalFilters = new JSONObject();
            JSONArray sites = new JSONArray();
            JSONObject sellerName = new JSONObject();
            JSONObject sellerId = new JSONObject();
            JSONObject ruleSet = new JSONObject();
            JSONArray sellerValue = new JSONArray();
            sellerValue.put(0, "SAMPLE");
            
            ruleSet.put("operator", "OR");
            JSONArray rules = new JSONArray();
            JSONObject rule1 = new JSONObject();
            rule1.put("value", new JSONObject("{multiValue: ['p1', 'p2']}"));
            rule1.put("operator", "anyOf");
            rule1.put("label", "Product");
            rules.put(0, rule1);
            ruleSet.put("rules", rules);

            sellerName.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerName.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            sellerId.put(Constants.GlobalObjectKeys.value, sellerValue);
            sellerId.put(Constants.GlobalObjectKeys.operator, Constants.GlobalOperator.noneOf);

            globalFilters.put("sellerId", sellerId);
            globalFilters.put("sites", sites);
            globalFilters.put("tenant", "FACL");
            globalFilters.put("sellerName", sellerName);
            globalFilters.put("checkIsActiveFlag", true);


            Map<String, Object> params = new HashMap<>();
            params.put("ruleset", Parsing.encodeToBase64(ruleSet.toString()));
            params.put("exclusions", Parsing.encodeToBase64("{}"));
            params.put("globalFilters", Parsing.encodeToBase64(globalFilters.toString()));

            Result result = session
                    .run("CALL promodef.processSet({ruleset:$ruleset,exclusions:$exclusions,globalFilters:$globalFilters}) YIELD applicableEntityId RETURN applicableEntityId", params);
            List<String> resultRecords = new ArrayList<>();
            resultRecords.add("o1");
            resultRecords.add("o2");
            List<String> records = result.stream().map(record -> record.get("applicableEntityId").asString()).collect(Collectors.toList());
            assertThat(records).isEqualTo(resultRecords);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
