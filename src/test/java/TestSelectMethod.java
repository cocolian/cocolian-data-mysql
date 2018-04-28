import org.cocolian.mysql.JdbcProtobufTemplate;
import org.cocolian.mysql.TestDataSource;
import org.cocolian.mysql.foo.Foo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSelectMethod {
    private static Logger logger = LoggerFactory.getLogger(TestSelectMethod.class);

    @Test
    public void getByPK() throws Exception {
        JdbcProtobufTemplate jdbc = new JdbcProtobufTemplate<Foo>(TestDataSource.getJdbcTemplate(), Foo.class);

        Foo.Builder foo = (Foo.Builder) jdbc.get("lxp1").toBuilder();
        logger.debug(foo.toString());

    }

}
