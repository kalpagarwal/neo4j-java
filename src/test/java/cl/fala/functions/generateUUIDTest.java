package cl.fala.functions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)





public class generateUUIDTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(GenerateUUID.class)
                .build();
    }

    @Test
    void GenerateUUIDV5WithNameSpace() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {

            // When
            String result = session.run( "RETURN promodef.generateUUID(['7931745', '11a7e456-c115-4efe-bc96-f362f0407856'], '6ba7b811-9dad-11d1-80b4-00c04fd430c8') AS result").single().get("result").asString();

            // Then
            assertThat(result).isEqualTo(( "d8aac045-fac8-5e08-aae2-385b77039db8" ));
        }
    }

    @Test
    void GenerateUUIDV5WithOutNameSpace() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {

            // When
            String result = session.run( "RETURN promodef.generateUUID(['7931745', '11a7e456-c115-4efe-bc96-f362f0407856'], '') AS result").single().get("result").asString();

            // Then
            assertThat(result).isEqualTo(( "d8aac045-fac8-5e08-aae2-385b77039db8" ));
        }
    }
    @Test
    void GenerateUUIDV4() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {

            // When
            String result = session.run( "RETURN promodef.generateUUID(null, null) AS result").single().get("result").asString();

            // Then
            assertThat(result).isNotEmpty();
        }
    }
}
