import org.cocolian.mysql.JdbcProtobufTemplate;
import org.cocolian.mysql.TestDataSource;
import org.cocolian.mysql.foo.Foo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;


public class TestInsertMethod {
    private static Logger logger = LoggerFactory.getLogger(TestInsertMethod.class);

    @Test
    public void insert() throws Exception {
        JdbcProtobufTemplate jdbc = new JdbcProtobufTemplate<Foo>(TestDataSource.getJdbcTemplate(), Foo.class);

        Foo.Builder foo = Foo.newBuilder();
        foo.setCol1("cocolian"+ UUID.randomUUID().toString());
        foo.setCol2(new Random().nextInt(Integer.MAX_VALUE));
        foo.setCol3(new Random().nextDouble());

        logger.debug(foo.toString());
        long ret = jdbc.insert(foo.build());
        logger.debug(String.valueOf(ret));

    }

}
