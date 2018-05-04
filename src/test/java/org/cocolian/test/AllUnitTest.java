package org.cocolian.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestInsertMethod.class,
        TestSelectMethod.class,
        TestUpdateMethod.class
})

public class AllUnitTest {
    /**
     * create database cocolian;
     * create table test_foo (col1 varchar(200) primary key,col2 int ,col3 decimal(16,2));
     */
}
