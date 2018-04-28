package org.cocolian.mysql;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

public class TestDataSource {
    private static DataSource getDS() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/cocolian?characterEncoding=gbk");
        dataSource.setUsername("root");
        dataSource.setPassword("");

        return dataSource;
    }

    public static JdbcTemplate getJdbcTemplate() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(getDS());
        return jdbcTemplate;
    }
}
