import org.cocolian.mysql.JdbcProtobufTemplate;
import org.cocolian.mysql.TestDataSource;
import org.cocolian.mysql.foo.Foo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


public class TestUpdateMethod {
    private static Logger logger = LoggerFactory.getLogger(TestUpdateMethod.class);

    @Test
    public void updateAll() throws Exception {
        JdbcProtobufTemplate jdbc = new JdbcProtobufTemplate<Foo>(TestDataSource.getJdbcTemplate(), Foo.class);

        Foo.Builder foo = (Foo.Builder) jdbc.get("lxp1").toBuilder();
        logger.debug(foo.toString());
        foo.setCol2(new Random().nextInt(Integer.MAX_VALUE));

        int ret = jdbc.update(foo.build());
        logger.debug(String.valueOf(ret));

    }

    @Test
    public void partialUpdate() throws Exception {
        JdbcProtobufTemplate jdbc = new JdbcProtobufTemplate<Foo>(TestDataSource.getJdbcTemplate(), Foo.class);

        Foo.Builder foo = (Foo.Builder) jdbc.get("lxp1").toBuilder();
        logger.debug(foo.toString());
        foo.setCol3(new Random().nextDouble());

        int ret = jdbc.partialUpdate(foo.build());
        logger.debug(String.valueOf(ret));

    }
}
