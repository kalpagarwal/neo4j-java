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





public class GetDateTimeEpochMillisTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(GetDateTimeEpochMillis.class)
                .build();
    }

    @Test
    void getMillisecondsForUTC() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {
            Long result = session.run( "RETURN promodef.getMilliseconds('2020-05-19 21:54:05 UTC') AS result").single().get("result").asLong();
            assertThat(String.valueOf(result)).isEqualTo("1589925245000");
        }
    }
    
    @Test
    void getMillisecondsForZ() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {

            Long result = session.run( "RETURN promodef.getMilliseconds('2020-05-19T21:54:05z') AS result").single().get("result").asLong();

            assertThat(String.valueOf(result)).isEqualTo("1589925245000");
        }
    }
}
