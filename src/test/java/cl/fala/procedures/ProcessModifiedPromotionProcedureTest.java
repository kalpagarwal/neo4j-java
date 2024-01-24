package cl.fala.procedures;

import cl.fala.procedures.exceptions.Messages;
import cl.fala.procedures.promotion.ProcessModifiedPromotionProcedure;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProcessModifiedPromotionProcedureTest {
    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Driver neo4jDriver;
    JSONObject defautSeller = new JSONObject();
    
    @BeforeAll
    void initializeNeo4j() {
        Neo4j neo4j = Neo4jBuilders
                .newInProcessBuilder()
                .withProcedure(ProcessModifiedPromotionProcedure.class)
                .build();
        this.neo4jDriver = GraphDatabase.driver(neo4j.boltURI(), driverConfig);
        setUpGraph();
        defautSeller.put("value", new JSONArray());
        defautSeller.put("operator", "anyOf");
    }

    /*
    Creates Graph:
    c1
    |
    c2
    |  \
    p1  p2
    | \  \
    o1 o2 o3
    */
    private void setUpGraph() {
        try (Session session = neo4jDriver.session();
             Transaction tx = session.beginTransaction()) {
            tx.run("CREATE (c1:Category{uuid:'c1',tenant:'FACL'})");
            tx.run("CREATE (c2:Category{uuid:'c2',tenant:'FACL'})");
            tx.run("CREATE (p1:Product{uuid:'p1',tenant:'FACL'})");
            tx.run("CREATE (p1:Product{uuid:'p2',tenant:'FACL'})");
            tx.run("CREATE (o1:Offering:FACL{uuid:'o1',offeringId:'oo1',sellerId:'FALABELLA',tenant:'FACL', sellerName: 'FALABELLA', isActive: true})");
            tx.run("CREATE (o2:Offering:FACL{uuid:'o2',offeringId:'oo2',sellerId:'SAMPLE',tenant:'FACL', isActive: true})");
            tx.run("CREATE (o3:Offering:SOCL{uuid:'o3',offeringId:'oo3',sellerId:'FALABELLA',tenant:'FACL', sellerName: 'FALABELLA'})");
            tx.run("CREATE (o4:Offering:SOCL{uuid:'o4',offeringId:'oo4',sellerId:'FALABELLA',tenant:'FACL', sellerName:'FALABELLA', isActive: false})");

            String createRelationshipsQuery = "MATCH (c1:Category{uuid:'c1'}) " +
                    "MATCH (c2:Category{uuid:'c2'}) " +
                    "MATCH (p1:Product{uuid:'p1'}) " +
                    "MATCH (p2:Product{uuid:'p2'}) " +
                    "MATCH (o1:Offering{uuid:'o1'}) " +
                    "MATCH (o2:Offering{uuid:'o2'}) " +
                    "MATCH (o3:Offering{uuid:'o3'}) " +
                    "MATCH (o4:Offering{uuid:'o4'}) " +
                    "MERGE (c1)-[:HAS]->(c2) " +
                    "MERGE (c2)-[:HAS]->(p1) " +
                    "MERGE (c2)-[:HAS]->(p2) " +
                    "MERGE (p1)-[:HAS]->(o1) " +
                    "MERGE (p1)-[:HAS]->(o2) " +
                    "MERGE (p1)-[:HAS]->(o4) " +
                    "MERGE (p2)-[:HAS]->(o3) ";
            tx.run(createRelationshipsQuery);

            tx.commit();
        }
    }

    @AfterAll
    void tearDown() {
        this.neo4jDriver.close();
    }

    @Test
    public void shouldThrowIfIdIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {
            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64("{}"));
            params.put("newPromotion", Parsing.encodeToBase64("{}"));

            session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains(Messages.PromotionExceptions.ID_NOT_FOUND);
    }

    @Test
    public void shouldThrowIfTenantIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {
            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains(Messages.PromotionExceptions.TENANT_NOT_FOUND);
    }

    @Test
    public void shouldThrowIfSellerIdIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {
            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");

            JSONObject newPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains(Messages.PromotionExceptions.SELLER_ID_NOT_FOUND);
    }

    @Test
    public void shouldThrowIfConditionsIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);

            JSONObject newPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);


            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains(Messages.PromotionExceptions.MISSING_CONDITIONS);
    }

    @Test
    public void shouldThrowIfActionsIsNotProvided() {
        Exception exception = null;

        try (Session session = neo4jDriver.session()) {
    
            JSONObject conditions = new JSONObject();
            conditions.put("ruleset", new JSONObject());
            conditions.put("exclusions", new JSONObject());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", conditions);

            JSONObject newPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);

            oldPromotion.put("conditions", conditions);

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId as applicableEntityIds RETURN applicableEntityIds", params);
        } catch (Exception e) {
            exception = e;
        }

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains(Messages.PromotionExceptions.MISSING_ACTIONS);
    }

    @Test
    public void shouldStreamResultIfSellerIdIsDifferent() {
        try (Session session = neo4jDriver.session()) {
            ArrayList<String> sellerIdOld = new ArrayList<>();
            ArrayList<String> sellerNameOld = new ArrayList<>();
            sellerIdOld.add("FALABELLA");
            sellerNameOld.add("FALABELLA");
            JSONObject oldSellerId = new JSONObject();
            oldSellerId.put("value", new JSONArray(sellerIdOld));
            oldSellerId.put("operator", "anyOf");


            JSONObject oldSellerNameObj = new JSONObject();
            oldSellerNameObj.put("value", new JSONArray(sellerNameOld));
            oldSellerNameObj.put("operator", "anyOf");

            ArrayList<String> sellerIdNew = new ArrayList<>();
            sellerIdNew.add("SAMPLE");
            JSONObject newSellerId = new JSONObject();
            newSellerId.put("value", new JSONArray(sellerIdNew));
            newSellerId.put("operator", "anyOf");

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", oldSellerId);
            oldPromotion.put("sellerName", oldSellerNameObj);
            oldPromotion.put("sites", defautSeller);

            oldPromotion.put("conditions", new JSONObject());
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", newSellerId);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);

            newPromotion.put("conditions", new JSONObject());
            newPromotion.put("actions", new JSONObject());

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo3");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo1");
            entry3.put("type", "ACTIONS");
            entry3.put("action", "REMOVE");
            expectedResult.add(entry3);
            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo3");
            entry4.put("type", "ACTIONS");
            entry4.put("action", "REMOVE");
            expectedResult.add(entry4);
            Map<String, String> entry5 = new HashMap<>();
            entry5.put("offeringId", "oo2");
            entry5.put("type", "CONDITIONS");
            entry5.put("action", "ADD");
            expectedResult.add(entry5);
            Map<String, String> entry6 = new HashMap<>();
            entry6.put("offeringId", "oo2");
            entry6.put("type", "ACTIONS");
            entry6.put("action", "ADD");
            expectedResult.add(entry6);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldStreamResultIfSitesIsDifferent() {
        try (Session session = neo4jDriver.session()) {
            ArrayList<String> sitesOld = new ArrayList<>();
            sitesOld.add("FACL");
            JSONObject oldSites = new JSONObject();
            oldSites.put("value", new JSONArray(sitesOld));
            oldSites.put("operator", "anyOf");

            ArrayList<String> sitesNew = new ArrayList<>();
            sitesNew.add("SOCL");
            JSONObject newSites = new JSONObject();
            newSites.put("value", new JSONArray(sitesNew));
            newSites.put("operator", "anyOf");

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", oldSites);
            oldPromotion.put("conditions", new JSONObject());
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("conditions", new JSONObject());
            newPromotion.put("actions", new JSONObject());
            newPromotion.put("sites", newSites);

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo2");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo1");
            entry3.put("type", "ACTIONS");
            entry3.put("action", "REMOVE");
            expectedResult.add(entry3);
            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo2");
            entry4.put("type", "ACTIONS");
            entry4.put("action", "REMOVE");
            expectedResult.add(entry4);
            Map<String, String> entry5 = new HashMap<>();
            entry5.put("offeringId", "oo3");
            entry5.put("type", "CONDITIONS");
            entry5.put("action", "ADD");
            expectedResult.add(entry5);
            Map<String, String> entry6 = new HashMap<>();
            entry6.put("offeringId", "oo3");
            entry6.put("type", "ACTIONS");
            entry6.put("action", "ADD");
            expectedResult.add(entry6);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
           }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldStreamResultIfSellerNameIsDifferent() {
        try (Session session = neo4jDriver.session()) {
            ArrayList<String> sellerNameOld = new ArrayList<>();
            sellerNameOld.add("FALABELLA");

            JSONObject oldSellerNameObj = new JSONObject();
            oldSellerNameObj.put("value", new JSONArray(sellerNameOld));
            oldSellerNameObj.put("operator", "anyOf");

            ArrayList<String> sellerIdNew = new ArrayList<>();
            sellerIdNew.add("SAMPLE");
            JSONObject newSellerId = new JSONObject();
            newSellerId.put("value", new JSONArray(sellerIdNew));
            newSellerId.put("operator", "anyOf");

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", oldSellerNameObj);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", new JSONObject());
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", newSellerId);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);
            newPromotion.put("conditions", new JSONObject());
            newPromotion.put("actions", new JSONObject());

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo3");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo1");
            entry3.put("type", "ACTIONS");
            entry3.put("action", "REMOVE");
            expectedResult.add(entry3);
            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo3");
            entry4.put("type", "ACTIONS");
            entry4.put("action", "REMOVE");
            expectedResult.add(entry4);
            Map<String, String> entry5 = new HashMap<>();
            entry5.put("offeringId", "oo2");
            entry5.put("type", "CONDITIONS");
            entry5.put("action", "ADD");
            expectedResult.add(entry5);
            Map<String, String> entry6 = new HashMap<>();
            entry6.put("offeringId", "oo2");
            entry6.put("type", "ACTIONS");
            entry6.put("action", "ADD");
            expectedResult.add(entry6);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void shouldStreamResultIfOldAndNewPromotionRulesAreCompletelyDifferent() {
        try (Session session = neo4jDriver.session()) {
            JSONObject oldRuleSet = new JSONObject();
            oldRuleSet.put("operator", "AND");
            JSONArray oldRules = new JSONArray();
            JSONObject oldRule1 = new JSONObject();
            oldRule1.put("value", new JSONObject("{singleValue:'p1'}"));
            oldRule1.put("operator", "isEqualTo");
            oldRule1.put("label", "Product");
            oldRules.put(0, oldRule1);
            oldRuleSet.put("rules", oldRules);

            JSONObject oldConditions = new JSONObject();
            oldConditions.put("ruleset", oldRuleSet);
            oldConditions.put("exclusions", new JSONObject());

            JSONObject newRuleSet = new JSONObject();
            newRuleSet.put("operator", "AND");
            JSONArray newRules = new JSONArray();
            JSONObject newRule1 = new JSONObject();
            newRule1.put("value", new JSONObject("{singleValue:'p2'}"));
            newRule1.put("operator", "isEqualTo");
            newRule1.put("label", "Product");
            newRules.put(0, newRule1);
            newRuleSet.put("rules", newRules);

            ArrayList<String> sitesNew = new ArrayList<>();
            sitesNew.add("FACL");
            JSONObject newSites = new JSONObject();
            newSites.put("value", new JSONArray(sitesNew));
            newSites.put("operator", "anyOf");


            JSONObject newConditions = new JSONObject();
            newConditions.put("ruleset", newRuleSet);
            newConditions.put("exclusions", new JSONObject());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", newSites);
            oldPromotion.put("conditions", oldConditions);
            oldPromotion.put("actions", new JSONObject());
            oldPromotion.put("shouldProcessActions", false);

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", newSites);
            newPromotion.put("conditions", newConditions);
            newPromotion.put("actions", new JSONObject());
            newPromotion.put("shouldProcessActions", false);

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo2");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo1");
            entry3.put("type", "ACTIONS");
            entry3.put("action", "REMOVE");
            expectedResult.add(entry3);
            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo2");
            entry4.put("type", "ACTIONS");
            entry4.put("action", "REMOVE");
            expectedResult.add(entry4);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldStreamResultIfSomeOfferingsFromOldPromotionAreRemoved() {
        try (Session session = neo4jDriver.session()) {
            JSONObject oldRuleSet = new JSONObject();
            oldRuleSet.put("operator", "AND");
            JSONArray oldRules = new JSONArray();
            JSONObject oldRule1 = new JSONObject();
            oldRule1.put("value", new JSONObject("{singleValue: 'C1'}"));
            oldRule1.put("operator", "isEqualTo");
            oldRule1.put("label", "Category");
            oldRules.put(0, oldRule1);
            oldRuleSet.put("rules", oldRules);

            JSONObject oldConditions = new JSONObject();
            oldConditions.put("ruleset", oldRuleSet);
            oldConditions.put("exclusions", new JSONObject());

            JSONObject newRuleSet = new JSONObject();
            newRuleSet.put("operator", "AND");
            JSONArray newRules = new JSONArray();
            JSONObject newRule1 = new JSONObject();
            newRule1.put("value",new JSONObject("{multiValue:['p2']}"));
            newRule1.put("operator", "anyOf");
            newRule1.put("label", "Product");
            newRules.put(0, newRule1);
            newRuleSet.put("rules", newRules);

            JSONObject newConditions = new JSONObject();
            newConditions.put("ruleset", newRuleSet);
            newConditions.put("exclusions", new JSONObject());

            JSONObject sellerId = new JSONObject();
            sellerId.put("value", new JSONArray());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", oldConditions);
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);
            newPromotion.put("conditions", newConditions);
            newPromotion.put("actions", new JSONObject());

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo2");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo3");
            entry3.put("type", "CONDITIONS");
            entry3.put("action", "ADD");
            
            expectedResult.add(entry3);
            expectedResult.add(entry2);
            expectedResult.add(entry1);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldStreamResultIfSomeVariantIdFromNewPromotionAreNotInNeo4j() {
        try (Session session = neo4jDriver.session()) {
            JSONObject sellerId = new JSONObject();
            sellerId.put("value", new JSONArray());

            JSONObject oldRuleSet = new JSONObject();
            oldRuleSet.put("operator", "AND");
            JSONArray oldRules = new JSONArray();
            JSONObject oldRule1 = new JSONObject();
            oldRule1.put("value", new JSONObject("{singleValue:'p1'}"));
            oldRule1.put("operator", "isEqualTo");
            oldRule1.put("label", "Product");
            oldRules.put(0, oldRule1);
            oldRuleSet.put("rules", oldRules);

            JSONObject oldConditions = new JSONObject();
            oldConditions.put("ruleset", oldRuleSet);
            oldConditions.put("exclusions", new JSONObject());

            JSONObject newRuleSet = new JSONObject();
            newRuleSet.put("operator", "AND");
            JSONArray newRules = new JSONArray();
            JSONObject newRule1 = new JSONObject();
            newRule1.put("value", new JSONObject("{singleValue: 'p9'}"));
            newRule1.put("operator", "isEqualTo");
            newRule1.put("label", "Product");
            newRules.put(0, newRule1);
            newRuleSet.put("rules", newRules);

            JSONObject newConditions = new JSONObject();
            newConditions.put("ruleset", newRuleSet);
            newConditions.put("exclusions", new JSONObject());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", oldConditions);
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);
            newPromotion.put("conditions", newConditions);
            newPromotion.put("actions", new JSONObject());

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo2");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    @Test
    public void shouldStreamResultIfSomeOfferingsFromOldPromotionAreAdded() {
        try (Session session = neo4jDriver.session()) {
            JSONObject sellerId = new JSONObject();
            sellerId.put("value", new JSONArray());

            JSONObject oldRuleSet = new JSONObject();
            oldRuleSet.put("operator", "AND");
            JSONArray oldRules = new JSONArray();
            JSONObject oldRule1 = new JSONObject();
            oldRule1.put("value", new JSONObject("{singleValue:'p1'}"));
            oldRule1.put("operator", "isEqualTo");
            oldRule1.put("label", "Product");
            oldRules.put(0, oldRule1);
            oldRuleSet.put("rules", oldRules);

            JSONObject oldConditions = new JSONObject();
            oldConditions.put("ruleset", oldRuleSet);
            oldConditions.put("exclusions", new JSONObject());

            JSONObject newRuleSet = new JSONObject();
            newRuleSet.put("operator", "AND");
            JSONArray newRules = new JSONArray();
            JSONObject newRule1 = new JSONObject();
            newRule1.put("value", new JSONObject("{singleValue:'c2'}"));
            newRule1.put("operator", "isEqualTo");
            newRule1.put("label", "Category");
            newRules.put(0, newRule1);
            newRuleSet.put("rules", newRules);

            JSONObject newConditions = new JSONObject();
            newConditions.put("ruleset", newRuleSet);
            newConditions.put("exclusions", new JSONObject());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", oldConditions);
            oldPromotion.put("actions", new JSONObject());

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);
            newPromotion.put("conditions", newConditions);
            newPromotion.put("actions", new JSONObject());

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "ADD");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo3");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "ADD");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo2");
            entry3.put("type", "CONDITIONS");
            entry3.put("action", "ADD");
            expectedResult.add(entry3);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldNotGenerateActionsPAMIfFlagIsSetToFalse() {
        try (Session session = neo4jDriver.session()) {
            ArrayList<String> sellerIdOld = new ArrayList<>();
            sellerIdOld.add("FALABELLA");
            JSONObject oldSellerId = new JSONObject();
            oldSellerId.put("value", new JSONArray(sellerIdOld));
            oldSellerId.put("operator", "anyOf");


            ArrayList<String> sellerIdNew = new ArrayList<>();
            sellerIdNew.add("SAMPLE");
            JSONObject newSellerId = new JSONObject();
            newSellerId.put("value", new JSONArray(sellerIdNew));
            newSellerId.put("operator", "anyOf");

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", oldSellerId);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", new JSONObject());
            oldPromotion.put("actions", new JSONObject());
            oldPromotion.put("shouldProcessActions", true);

            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", newSellerId);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);

            newPromotion.put("conditions", new JSONObject());
            newPromotion.put("actions", new JSONObject());
            newPromotion.put("shouldProcessActions", false);

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            expectedResult.add(entry1);
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo3");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
            expectedResult.add(entry2);
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo1");
            entry3.put("type", "ACTIONS");
            entry3.put("action", "REMOVE");
            expectedResult.add(entry3);
            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo3");
            entry4.put("type", "ACTIONS");
            entry4.put("action", "REMOVE");
            expectedResult.add(entry4);
            Map<String, String> entry5 = new HashMap<>();
            entry5.put("offeringId", "oo2");
            entry5.put("type", "CONDITIONS");
            entry5.put("action", "ADD");
            expectedResult.add(entry5);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldStreamResultForModificationWithActiveFlag() {
        try (Session session = neo4jDriver.session()) {
            JSONObject oldRuleSet = new JSONObject();
            oldRuleSet.put("operator", "AND");
            JSONArray oldRules = new JSONArray();
            JSONObject oldRule1 = new JSONObject();
            oldRule1.put("value", new JSONObject("{singleValue: 'C1'}"));
            oldRule1.put("operator", "isEqualTo");
            oldRule1.put("label", "Category");
            oldRules.put(0, oldRule1);
            oldRuleSet.put("rules", oldRules);

            JSONObject oldConditions = new JSONObject();
            oldConditions.put("ruleset", oldRuleSet);
            oldConditions.put("exclusions", new JSONObject());

            JSONObject newRuleSet = new JSONObject();
            newRuleSet.put("operator", "AND");
            JSONArray newRules = new JSONArray();
            JSONObject newRule1 = new JSONObject();
            newRule1.put("value",new JSONObject("{multiValue:['p2']}"));
            newRule1.put("operator", "anyOf");
            newRule1.put("label", "Product");
            newRules.put(0, newRule1);
            newRuleSet.put("rules", newRules);

            JSONObject newConditions = new JSONObject();
            newConditions.put("ruleset", newRuleSet);
            newConditions.put("exclusions", new JSONObject());

            JSONObject sellerId = new JSONObject();
            sellerId.put("value", new JSONArray());

            JSONObject oldPromotion = new JSONObject();
            oldPromotion.put("id", "123");
            oldPromotion.put("tenant", "FACL");
            oldPromotion.put("sellerId", defautSeller);
            oldPromotion.put("sellerName", defautSeller);
            oldPromotion.put("sites", defautSeller);
            oldPromotion.put("conditions", oldConditions);
            oldPromotion.put("actions", new JSONObject());
            oldPromotion.put(Constants.GlobalObjectKeys.checkIsActiveFlag, false);


            JSONObject newPromotion = new JSONObject();
            newPromotion.put("id", "123");
            newPromotion.put("tenant", "FACL");
            newPromotion.put("sellerId", defautSeller);
            newPromotion.put("sellerName", defautSeller);
            newPromotion.put("sites", defautSeller);
            newPromotion.put("conditions", newConditions);
            newPromotion.put("actions", new JSONObject());
            oldPromotion.put(Constants.GlobalObjectKeys.checkIsActiveFlag, false);

            Map<String, Object> params = new HashMap<>();
            params.put("oldPromotion", Parsing.encodeToBase64(oldPromotion.toString()));
            params.put("newPromotion", Parsing.encodeToBase64(newPromotion.toString()));

            List<Map<String, String>> expectedResult = new ArrayList<>();
            Map<String, String> entry1 = new HashMap<>();
            entry1.put("offeringId", "oo1");
            entry1.put("type", "CONDITIONS");
            entry1.put("action", "REMOVE");
            
            Map<String, String> entry2 = new HashMap<>();
            entry2.put("offeringId", "oo2");
            entry2.put("type", "CONDITIONS");
            entry2.put("action", "REMOVE");
        
            
            Map<String, String> entry3 = new HashMap<>();
            entry3.put("offeringId", "oo3");
            entry3.put("type", "CONDITIONS");
            entry3.put("action", "ADD");
            

            Map<String, String> entry4 = new HashMap<>();
            entry4.put("offeringId", "oo4");
            entry4.put("type", "CONDITIONS");
            entry4.put("action", "REMOVE");
        
            
            expectedResult.add(entry3);
            expectedResult.add(entry4);
            expectedResult.add(entry2);
            expectedResult.add(entry1);

            List<Map<String, Object>> result = session.run("CALL promodef.getOfferingsForModifiedPromotion({oldPromotion:$oldPromotion,newPromotion:$newPromotion}) YIELD offeringId,type,action RETURN offeringId,type,action", params)
                    .stream().map(Record::asMap).collect(Collectors.toList());
            for (int i = 0; i < result.size(); i++) {
                Map<String, Object> map = result.get(i);
                assertThat(String.valueOf(map.get("offeringId"))).isEqualTo(expectedResult.get(i).get("offeringId"));
                assertThat(String.valueOf(map.get("type"))).isEqualTo(expectedResult.get(i).get("type"));
                assertThat(String.valueOf(map.get("action"))).isEqualTo(expectedResult.get(i).get("action"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
