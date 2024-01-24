package cl.fala.procedures;

import cl.fala.procedures.catalogDeltaChanges.GetOfferingEntityMapV1;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GetOfferingEntityMapTest {
    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Driver neo4jDriver;

    @BeforeAll
    void initializeNeo4j() {
        Neo4j neo4j = Neo4jBuilders
                .newInProcessBuilder()
                .withProcedure(GetOfferingEntityMapV1.class)
                .build();
        this.neo4jDriver = GraphDatabase.driver(neo4j.boltURI(), driverConfig);
        setUpGraph();
    }

    /*
    Creates Graph:
    c1
    |
    c2
    |
    p1
    | \
    o1 o2
    */
    private void setUpGraph() {
        try (Session session = neo4jDriver.session();
             Transaction tx = session.beginTransaction()) {
            tx.run("CREATE (c1:Category{uuid:'c1',tenant:'FACL'})");
            tx.run("CREATE (c2:Category{uuid:'c2',tenant:'FACL'})");
            tx.run("CREATE (p1:Product:FACL{uuid:'p1',tenant:'FACL'})");
            tx.run("CREATE (p1:ProductType {uuid:'pt1',tenant:'FACL',productType: 'pt1'})");
            tx.run("CREATE (v1:Variant {uuid:'v1',tenant:'FACL',productType: 'pt1'})");
            tx.run("CREATE (v2:Variant {uuid:'v2',tenant:'FACL'})");
            tx.run("CREATE (o1:Offering:FACL{uuid:'o1',offeringId:'oo1',sellerId:'FALABELLA',tenant:'FACL', sellerName: 'FALABELLA'})");
            tx.run("CREATE (o2:Offering:FACL{uuid:'o2',offeringId:'oo2',sellerId:'SAMPLE',tenant:'FACL'})");

            String createRelationshipsQuery = "MATCH (c1:Category{uuid:'c1'}) with c1 " +
                    "MATCH (c2:Category{uuid:'c2'}) with c1, c2 " +
                    "MATCH (p1:Product{uuid:'p1'}) with c1,c2,p1 " +
                    "MATCH (o1:Offering{uuid:'o1'}) with c1,c2,p1,o1 " +
                    "MATCH (o2:Offering{uuid:'o2'}) with c1,c2,p1,o1,o2 " +
                    "MATCH (v1:Variant{uuid:'v1'}) with c1,c2,p1,o1,o2,v1 " +
                    "MATCH (v2:Variant{uuid:'v2'}) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (c1)-[:HAS]->(c2) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (c2)-[:HAS]->(p1) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (p1)-[:HAS]->(v1) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (p1)-[:HAS]->(v2) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (v1)-[:HAS]->(o1) with c1,c2,p1,o1,o2,v1,v2 " +
                    "CREATE (v2)-[:HAS]->(o2) ";
            tx.run(createRelationshipsQuery);

            tx.commit();
        }
    }

    @AfterAll
    void tearDown() {
        this.neo4jDriver.close();
    }

    @Test
    public void shouldGetProperConnectionMap() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (c2:Category{uuid:'c2'}) with c2 "+ 
                "MATCH (c1:Category{uuid:'c1'}) with c1 ,c2 "+
                "MATCH (c1)-[r:HAS]->(c2) with c1,c2,id(r) as excludePath "+
                "MATCH (p1:Product{uuid:'p1'}) with p1,c1,c2,excludePath "+
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:c2,toNodes:[c1,p1],excludePaths:{relationshipIds:[excludePath]}}) YIELD result RETURN result";

            result = session.run(query);

            

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("SAMPLE");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("null");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");

            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldGetProperConnectionMapWithoutExcludedPath() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (fromNode:Category{uuid:'c2'}) with fromNode " +
                "MATCH (toNode:Product{uuid:'p1'}) with toNode, fromNode " +
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:fromNode,toNodes:[toNode]}) YIELD result RETURN result";

            result = session.run(query);

            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("SAMPLE");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("null");
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");

        } catch (Exception e) {
            fail(e.getMessage());
        } 
    }
    @Test
    public void shouldGetProperConnectionMapWithExcludedPathAsNull() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (c2:Category{uuid:'c2'}) with c2 "+ 
                "MATCH (c1:Category{uuid:'c1'}) with c2,c1 "+
                "MATCH (p1:Product{uuid:'p1'}) with c1,c2,p1 "+
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:p1,toNodes:[c1,c2,p1], excludePaths:null}) YIELD result RETURN result";
            
            result = session.run(query);

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("SAMPLE");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("null");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("c2").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("c2").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");

        } catch (Exception e) {
            fail(e.getMessage());
        }
        
    }
   @Test
    public void shouldGetProperConnectionMapWithExcludedPathAndExcludeNode() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (c2:Category{uuid:'c2'}) with c2 "+ 
                "MATCH (c1:Category{uuid:'c1'}) with c1 ,c2 "+
                "MATCH (c1)-[r:HAS]-(c2) with c1,c2,id(r) as excludePath "+
                "MATCH (p1:Product{uuid:'p1'}) with p1,c1,c2,excludePath "+
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:c2,toNodes:[c1,p1],excludePaths:{relationshipIds:[excludePath], excludeFromNode: true}}) YIELD result RETURN result";

            result = session.run(query);

            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");
            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsExactly("FACL");

        } catch (Exception e) {
            fail(e.getMessage());
        }
        
    }

    @Test
    public void shouldGetProperConnectionMapForProductType() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (c2:Category{uuid:'c2'}) with c2 "+ 
                "MATCH (c1:Category{uuid:'c1'}) with c1 ,c2 "+
                "MATCH (c1)-[r:HAS]->(c2) with c1,c2,id(r) as excludePath "+
                "MATCH (p1:Product{uuid:'p1'}) with p1,c1,c2,excludePath "+
                "MATCH (pt1:ProductType{uuid:'pt1'}) with p1,c1,c2,excludePath,pt1 "+
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:c2,toNodes:[c1,p1,pt1],excludePaths:{relationshipIds:[excludePath]}}) YIELD result RETURN result";

            result = session.run(query);

            

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("SAMPLE");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("null");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("pt1").asBoolean()).isEqualTo(false);

            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");

            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("pt1").asBoolean()).isEqualTo(true);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void shouldGetProperConnectionMapForExcludeProductType() {
        try (Session session = neo4jDriver.session()) {
            Result result;
            Value recordValue;
            String query = "MATCH (c2:Category{uuid:'c2'}) with c2 "+ 
                "MATCH (c1:Category{uuid:'c1'}) with c1 ,c2 "+
                "MATCH (c1)-[r:HAS]->(c2) with c1,c2,id(r) as excludePath "+
                "MATCH (p1:Product{uuid:'p1'}) with p1,c1,c2,excludePath "+
                "MATCH (pt1:ProductType{uuid:'pt1'}) with p1,c1,c2,excludePath,pt1 "+
                "CALL promodef.getOfferingEntityConnectionMapV1({fromNode:c2,toNodes:[c1,p1,pt1],excludePaths:{relationshipIds:[excludePath], excludeProductType: 'pt1'}}) YIELD result RETURN result";

            result = session.run(query);

            

            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo2");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("SAMPLE");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("null");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");
            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("pt1").asBoolean()).isEqualTo(false);

            
            recordValue = result.next().get("result");
            assertThat(recordValue.get("offeringId").asString()).isEqualTo("oo1");
            assertThat(recordValue.get("sellerId").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get("sellerName").asString()).isEqualTo("FALABELLA");
            assertThat(recordValue.get(Constants.GRAPH_NODE_PROPERTIES.sites).asList()).containsOnly("FACL");

            assertThat(recordValue.get("entityConnectionMap").get("c1").asBoolean()).isEqualTo(false);
            assertThat(recordValue.get("entityConnectionMap").get("p1").asBoolean()).isEqualTo(true);
            assertThat(recordValue.get("entityConnectionMap").get("pt1").asBoolean()).isEqualTo(false);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

}
