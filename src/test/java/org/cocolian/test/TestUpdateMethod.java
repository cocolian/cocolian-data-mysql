package org.cocolian.test;

import org.cocolian.mysql.JdbcProtobufTemplate;
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

        Foo.Builder foo = (Foo.Builder) jdbc.get("cocolian38b8b3fb-4e4d-467b-8e7e-935f4838a9bc").toBuilder();
        logger.debug(foo.toString());
        foo.setCol2(new Random().nextInt(Integer.MAX_VALUE));

        int ret = jdbc.update(foo.build());
        logger.debug(String.valueOf(ret));

    }

    @Test
    public void partialUpdate() throws Exception {
        JdbcProtobufTemplate jdbc = new JdbcProtobufTemplate<Foo>(TestDataSource.getJdbcTemplate(), Foo.class);
        Foo.Builder foo = Foo.newBuilder();
        foo.setCol1("cocolian38b8b3fb-4e4d-467b-8e7e-935f4838a9bc");
        foo.setCol3(new Random().nextDouble());
        logger.debug(foo.toString()+foo.getCol2());
        int ret = jdbc.partialUpdate(foo.build());
        logger.debug(String.valueOf(ret));

    }
}
