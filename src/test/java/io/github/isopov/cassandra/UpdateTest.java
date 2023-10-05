package io.github.isopov.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class UpdateTest {

    @Container
    private static final CassandraContainer cassandra
            = (CassandraContainer) new CassandraContainer("cassandra:4.1.3").withExposedPorts(9042);

    private static CqlSession session;

    @BeforeAll
    static void setup() {
        session = CqlSession.builder()
                .addContactPoint(cassandra.getContactPoint())
                .withLocalDatacenter("datacenter1")
                .build();

        session.execute("drop keyspace if exists updatetests");
        session.execute("create keyspace updatetests with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        session.execute("use updatetests");
        session.execute("drop table if exists test");
        session.execute("create table test (a text, b int, primary key(a))");
    }

    @AfterAll
    static void tearDown() {
        session.close();
    }

    @Test
    void testLwtInsertAndLwtUpdate() {
        session.execute("insert into test(a,b) values(?,?) if not exists", "x", 1);
        session.execute("update test set b=? where a=? if exists", 2, "x");
        assertUpdate("x");
    }

    @Test
    void testInsertAndLwtUpdate() {
        session.execute("insert into test(a,b) values(?,?)", "y", 1);
        session.execute("update test set b=? where a=? if exists", 2, "y");
        assertUpdate("y");
    }

    @Test
    void testLwtInsertAndUpdate() {
        session.execute("insert into test(a,b) values(?,?) if not exists", "z", 1);
        session.execute("update test set b=? where a=?", 2, "z");
        assertUpdate("z");
    }
    private static void assertUpdate(String id) {
        var res = session.execute("select * from test where a=?", id);
        res.forEach(row -> assertEquals(2, row.getInt("b")));
    }
}