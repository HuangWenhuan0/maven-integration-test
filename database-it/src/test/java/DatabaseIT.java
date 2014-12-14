import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.jcabi.jdbc.JdbcSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by huangwenhuan on 14-12-14.
 */
public class DatabaseIT {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * MySQL port.
     */
    private static final String PORT =
            System.getProperty("mysql.port");

    /**
     * It is not a default MySQL port.
     * @throws Exception If something is wrong
     */
    @Test
    public void itIsCustomMySqlPort() throws Exception {
        assertThat(PORT, not(equalTo("3306")));
    }

    @Test
    public void testSql() throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        String url = String.format(
                "jdbc:mysql://127.0.0.1:%s/test_schema?user=root&password=root",
                PORT);
        Connection conn = DriverManager.getConnection(url);
        Integer columnCount = new JdbcSession(conn)
                .autocommit(false)
                .sql("INSERT INTO person (name, age, sex, phone) " +
                        "VALUES ('aha', '100', 'intersex', '18600000000')")
                .execute()
                .sql("SELECT id, name, sex FROM person")
                .select(new JdbcSession.Handler<Integer>() {
                    @Override
                    public Integer handle(ResultSet rset) throws SQLException {
                        ResultSetMetaData rsetMeta = rset.getMetaData();
                        int columnCount = rsetMeta.getColumnCount();
                        logger.info("ResetSet column count is equal to {}.", columnCount);

                        while (rset.next()) {
                            for (int index = 1; index <= columnCount; index++) {
                                logger.info(
                                        "{} is equal to {}",
                                        rsetMeta.getColumnLabel(index), rset.getObject(index).toString());
                            }
                        }
                        return columnCount;
                    }
                });
        assertThat(columnCount, is(equalTo(3)));
    }
}
