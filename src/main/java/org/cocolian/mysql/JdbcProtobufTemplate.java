package org.cocolian.mysql;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.cocolian.mysql.taglib.ColumnFieldOption;
import org.cocolian.mysql.taglib.ColumnType;
import org.cocolian.mysql.taglib.TableMessageOption;
import org.cocolian.mysql.taglib.Taglib;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.Map.Entry;

/**
 * 存取Protobuf message 数据对象到JDBC数据库中， 是对<code>JdbcTemplate</code>的一个封装。
 * 要求对存储的对象，使用protobuf来定义， 并通过option来注释相关内容：<ul>
 * <li>table: 在message上，用来标记所在的表名</li>
 * <li>column-name : 在field上，用来标记存储的列</li>
 * <li>primary-key: 在field上，用来标记主键</li>
 * </ul>
 *
 * @param <M> message的数据类型
 * @author shamphone@gmail.com
 * @version 1.0.0
 * @date 2017年8月9日
 */
public class JdbcProtobufTemplate<M extends Message> {

    private static Logger logger = LoggerFactory.getLogger(JdbcProtobufTemplate.class);


    /**
     * 数据库行映射到 protobuf message对象
     */
    public class ProtobufMessageRowMapper<N extends Message> implements RowMapper<N> {
        @SuppressWarnings("unchecked")
        @Override
        public N mapRow(ResultSet rs, int rowNum) throws SQLException {
            Message.Builder builder = newBuilder(messageClass);
            populate(rs, builder);
            return (N) builder.build();
        }
    }

    private Class<M> messageClass;
    private Descriptors.Descriptor descriptor;
    private JdbcTemplate jdbcTemplate;
    private String _tableName = null;
    private String _primaryKeyName = null;

    public JdbcProtobufTemplate(JdbcTemplate jdbcTemplate, Class<M> messageClass) {
        this.jdbcTemplate = jdbcTemplate;
        if (messageClass == null) {
            this.messageClass = this.parseMessageClass();
        } else {
            this.messageClass = messageClass;
        }
        this.descriptor = this.getDescriptor(messageClass);
    }

    public JdbcProtobufTemplate(JdbcTemplate jdbcTemplate) {
    }


    @SuppressWarnings("unchecked")
    private Class<M> parseMessageClass() {
        Type genType = getClass().getGenericSuperclass();
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
        for (Type type : params) {// 查找 message类
            if (type instanceof Class && Message.class.isAssignableFrom((Class<?>) type)) {
                return (Class<M>) type;
            }
        }
        return null;
    }

    protected synchronized String getTableName(M message) {
        if (this._tableName == null) {
            TableMessageOption tableMessageOption = descriptor.getOptions().getExtension(Taglib.tableOption);
            // 默认从protobuf配置中查询表名
            if (tableMessageOption != null && StringUtils.isNotBlank(tableMessageOption.getTableName())) {
                this._tableName = tableMessageOption.getTableName();
            }
        }
        return this._tableName;
    }

    protected synchronized String getPrimaryKeyName(M message) {
        if (this._primaryKeyName == null) {
            TableMessageOption tableMessageOption = descriptor.getOptions().getExtension(Taglib.tableOption);
            // 默认从protobuf配置中查询主健名
            if (tableMessageOption != null && StringUtils.isNotBlank(tableMessageOption.getPrimaryKey())) {
                this._primaryKeyName = tableMessageOption.getPrimaryKey();
            }
        }
        return this._primaryKeyName;
    }

    private Descriptors.Descriptor getDescriptor(Class<M> messageClass) {
        try {
            return (Descriptors.Descriptor) MethodUtils.invokeStaticMethod(messageClass, "getDescriptor");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            throw new InvalidPropertyException(messageClass, "descriptor", "Could not found getDescriptor method.");
        }
    }

    /**
     * 查询单个记录
     *
     * @param sql
     * @param args
     * @return
     */
    public M get(String sql, Object... args) {
        logger.debug(sql);
        try {
            return jdbcTemplate.queryForObject(sql, new ProtobufMessageRowMapper<M>(), args);
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    /**
     * 查询单个记录
     *
     * @param sql
     * @param args
     * @return
     */
    public M get(String sql, final List<?> args) {
        logger.debug(sql);
        try {
            return jdbcTemplate.queryForObject(sql, new ProtobufMessageRowMapper<M>(), args.toArray());
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }


    /**
     * 根据主健查询单记录
     *
     * @param <V>
     * @param primaryKeyValue
     * @return
     */
    public <V> M get(V primaryKeyValue) {
        StringBuilder selectSql = new StringBuilder("select * from ");
        selectSql.append(getTableName(null));
        StringBuilder conditionSql = new StringBuilder(" where ");
        List<Object> args = new ArrayList<Object>();

        if (null != getPrimaryKeyName(null) && (null != primaryKeyValue && !"".equalsIgnoreCase(primaryKeyValue.toString()))) {
            //if has primary-key && fields-val is not null then get obj by primary-key
            conditionSql.append(getPrimaryKeyName(null)).append("=").append("?");
            args.add(primaryKeyValue);
        } else {
            //else get obj which fields has values
            logger.error("This ProtoBuf file doesn't set 'primary_key' field ,Please check it now.");
            return null;
        }

        selectSql.append(conditionSql);
        return (M) get(selectSql.toString(), args);
    }


    /**
     * 查询多个记录
     *
     * @param sql
     * @param args
     * @return
     */
    public List<M> query(String sql, Object... args) {
        logger.debug(sql);
        return jdbcTemplate.query(sql, new ProtobufMessageRowMapper<M>(), args);
    }


    /**
     * update method
     */
    public int update(String sql, Object... args) {
        logger.debug(sql);
        return jdbcTemplate.update(sql, args);
    }

    /**
     * @param sql
     * @param args
     * @return
     */
    public int update(final String sql, final List<?> args) {
        if (logger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("{sql: \"").append(sql).append("\"; parameters:").append(args);
            logger.debug(builder.toString());
        }
        return jdbcTemplate.update(new PreparedStatementCreator() {

            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement ps = con.prepareStatement(sql);
                populate(ps, args);
                return ps;
            }

        });
    }

    /**
     * 根据主健获取到的对象进行全量字段更新
     * @param message
     * @return
     */
    public int update(M message) {
        StringBuilder updateSql = new StringBuilder("update ");
        updateSql.append(getTableName(message)).append(" set ");

        StringBuilder conditionSql = new StringBuilder(" where ");
        conditionSql.append(getPrimaryKeyName(message)).append(" = ?");

        StringBuilder fields = new StringBuilder("");
        List<Object> args = new ArrayList<Object>();
        List<Object> keyArgs = new ArrayList<>();
        Map<Descriptors.FieldDescriptor, Object> fieldMap = message.getAllFields();

        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fieldMap.entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
            DescriptorProtos.FieldOptions fieldOptions = fieldDescriptor.getOptions();
            ColumnFieldOption columnFieldOption = fieldOptions.getExtension(Taglib.columnOption);
            String fieldName = fieldDescriptor.getName();
            Object value = entry.getValue();

            if (fieldName.trim().equalsIgnoreCase(getPrimaryKeyName(message))) {
                keyArgs.add(value);
                continue;
            }

            fields.append(fieldName).append("=?, ");
            args.add(convertColumnVal(columnFieldOption,value));
        }
        int tmpIndex = fields.lastIndexOf(",");
        updateSql.append(fields.substring(0, tmpIndex));
        updateSql.append(conditionSql.toString());

        args.addAll(keyArgs);
        return update(updateSql.toString(),args);
    }

    /**
     * 根据定义列类型进行数据值格式转换
     * @param columnFieldOption
     * @param value
     * @return
     */
    private Object convertColumnVal(ColumnFieldOption columnFieldOption,Object value) {
        if (columnFieldOption.getColumnType() == ColumnType.DATETIME
                || columnFieldOption.getColumnType() == ColumnType.TIMESTAMP) {// datetime类型
            if (value != null && (long) value > 0) {
                return new Timestamp((long) value);
            }
        }
        return value;
    }

    /**
     * 仅更新对象中有设置值的部分
     * @param message
     * @return
     */
    public int partialUpdate(M message){
        StringBuilder updateSql = new StringBuilder("update ");
        updateSql.append(getTableName(message)).append(" set ");

        StringBuilder conditionSql = new StringBuilder(" where ");
        conditionSql.append(getPrimaryKeyName(message)).append(" =?");

        StringBuilder selectSql = new StringBuilder("select * from ");
        selectSql.append(getTableName(message));

        selectSql.append(conditionSql);

        StringBuilder fields = new StringBuilder("");
        Map<Object,Object> paramsMap = new LinkedHashMap<>();
        List<Object> keyArgs = new ArrayList<>();

        Map<Descriptors.FieldDescriptor, Object> fieldMap = message.getAllFields();

        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fieldMap.entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
            DescriptorProtos.FieldOptions fieldOptions = fieldDescriptor.getOptions();
            ColumnFieldOption columnFieldOption = fieldOptions.getExtension(Taglib.columnOption);
            String fieldName = fieldDescriptor.getName();
            Object value = entry.getValue();

            if (columnFieldOption.getColumnType() == ColumnType.DATETIME
                    || columnFieldOption.getColumnType() == ColumnType.TIMESTAMP) {// datetime类型
                if (value != null && (long) value > 0) {
                    paramsMap.put(fieldName, new Timestamp((long) value));
                }
            } else {
                paramsMap.put(fieldName, value);
            }

        }

        keyArgs.add(paramsMap.get(getPrimaryKeyName(message)));
        M dbObj = get(selectSql.toString(),keyArgs);

        List<Object> args = new ArrayList<Object>();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : dbObj.getAllFields().entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
            String fieldName = fieldDescriptor.getName();
            Object value = entry.getValue();

            if(!paramsMap.get(fieldName).equals(value)){
                fields.append(fieldName).append("=?, ");
                args.add(paramsMap.get(fieldName));
            }
        }
        if(args.isEmpty()){
            //no data change
            logger.debug("no data change.");
            return 0;
        }

        int tmpIndex = fields.lastIndexOf(",");
        updateSql.append(fields.substring(0, tmpIndex));
        updateSql.append(conditionSql.toString());

        args.addAll(keyArgs);
        return update(updateSql.toString(),args);
    }

    /**
     * set preparedstatement params
     *
     * @param ps
     * @param args
     * @return
     * @throws SQLException
     */
    private void populate(PreparedStatement ps, List<?> args) throws SQLException {
        for (int i = 0; i < args.size(); i++) {
            Object o = args.get(i);
            if (o instanceof Integer) {
                ps.setInt(i + 1, (int) o);
            } else if (o instanceof Long) {
                ps.setLong(i + 1, (long) o);
            } else if (o instanceof String) {
                ps.setString(i + 1, (String) o);
            } else if (o instanceof Date) {
                ps.setDate(i + 1, (Date) o);
            } else if (o instanceof Float) {
                ps.setFloat(i + 1, (Float) o);
            } else if (o instanceof Double) {
                ps.setDouble(i + 1, (Double) o);
            } else if (o instanceof Timestamp) {
                ps.setTimestamp(i + 1, (Timestamp) o);
            } else if (o instanceof Descriptors.EnumValueDescriptor) {
                ps.setInt(i + 1, ((Descriptors.EnumValueDescriptor) o).getNumber());
            } else if (o instanceof Boolean) {
                ps.setBoolean(i + 1, (Boolean) o);
            } else {
                ps.setObject(i + 1, o);
            }
        }
    }

    /**
     * @param rs
     * @param builder
     * @throws SQLException
     */
    private void populate(ResultSet rs, Message.Builder builder) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();// 列个数
        String columnLabel = null;// 列名
        Object columnValue = null;// 列值
        Descriptors.FieldDescriptor fieldDescriptor = null;
        for (int i = 1; i <= columnCount; i++) {
            columnLabel = metaData.getColumnLabel(i);
            columnValue = rs.getObject(i);
            if (columnValue == null)
                continue;// 如果为空，继续下一个
            fieldDescriptor = descriptor.findFieldByName(columnLabel);
            if (fieldDescriptor == null)
                continue;// 如果为空，继续下一个
            // 转换为相应的类型 ，会自动将 date 类型转换为long
            if (fieldDescriptor.getType().equals(FieldDescriptor.Type.ENUM)) {
                columnValue = fieldDescriptor.getEnumType().findValueByNumber((int) columnValue);
            } else {
                columnValue = ConvertUtils.convert(columnValue, fieldDescriptor.getDefaultValue().getClass());
            }
            builder.setField(fieldDescriptor, columnValue);
        }
    }

    /**
     * 插入对象到默认的表中
     *
     * @param message
     * @return
     */
    public long insert(M message) {
        return insert(message, this.getTableName(message));
    }

    /**
     * 插入对象到给定的表中。注意，这里并不自动生成ID。
     *
     * @param message
     * @param tableName
     * @return
     */
    protected long insert(M message, String tableName) {
        StringBuilder insertSql = new StringBuilder("insert into ");
        insertSql.append(tableName).append("(");
        StringBuilder fields = new StringBuilder("");
        StringBuilder values = new StringBuilder("");
        List<Object> args = new ArrayList<Object>();
        Map<FieldDescriptor, Object> fieldMap = message.getAllFields();

        for (Entry<FieldDescriptor, Object> entry : fieldMap.entrySet()) {
            FieldDescriptor fieldDescriptor = entry.getKey();
            FieldOptions fieldOptions = fieldDescriptor.getOptions();
            ColumnFieldOption columnFieldOption = fieldOptions.getExtension(Taglib.columnOption);
            String fieldName = fieldDescriptor.getName();
            Object value = entry.getValue();

            if (columnFieldOption.getColumnType() == ColumnType.DATETIME
                    || columnFieldOption.getColumnType() == ColumnType.TIMESTAMP) {// datetime类型
                if (value != null && (long) value > 0) {
                    fields.append('`').append(fieldName).append("`,");
                    values.append("?, ");
                    args.add(new Timestamp((long) value));
                }
            } else {
                fields.append('`').append(fieldName).append("`,");
                values.append("?, ");
                args.add(value);
            }
        }
        int tmpIndex = fields.lastIndexOf(",");
        insertSql.append(fields.substring(0, tmpIndex)).append(") values(");
        tmpIndex = values.lastIndexOf(",");
        insertSql.append(values.substring(0, tmpIndex)).append(")");
        String sql = insertSql.toString();
        return update(sql, args);
    }


    /**
     * @param message
     * @param conditionFields
     * @param conditionParams
     * @param tableName
     * @return
     */
    protected int updateMessageByCondition(M message, String[] conditionFields, Object[] conditionParams,
                                           String tableName) {
        StringBuilder updateSql = new StringBuilder("update ");
        updateSql.append(tableName).append(" set ");
        StringBuilder options = new StringBuilder("");
        List<Object> args = new ArrayList<Object>();
        Map<FieldDescriptor, Object> fieldMap = message.getAllFields();

        for (Entry<FieldDescriptor, Object> entry : fieldMap.entrySet()) {
            FieldDescriptor fieldDescriptor = entry.getKey();
            if (!Arrays.asList(conditionFields).contains(fieldDescriptor.getName())) {
                FieldOptions fieldOptions = fieldDescriptor.getOptions();
                ColumnFieldOption columnFieldOption = fieldOptions.getExtension(Taglib.columnOption);
                String fieldName = fieldDescriptor.getName();
                Object value = entry.getValue();
                if (columnFieldOption.getColumnType() == ColumnType.DATETIME
                        || columnFieldOption.getColumnType() == ColumnType.TIMESTAMP) {// datetime类型
                    if (value != null && (long) value > 0) {
                        options.append(fieldName).append("=?, ");
                        args.add(new Timestamp((long) value));
                    }
                } else {
                    options.append(fieldName).append("=?, ");
                    args.add(value);
                }
            }
        }
        int tmpIndex = options.lastIndexOf(",");
        updateSql.append(options.substring(0, tmpIndex)).append(" where 1=1 ");
        StringBuilder condition = new StringBuilder();
        if (conditionFields.length != conditionParams.length) {
            throw new IllegalArgumentException("condition error");
        } else {
            for (int i = 0; i < conditionFields.length; i++) {
                condition.append("AND ").append(conditionFields[i]).append("=? ");
                args.add(conditionParams[i]);
            }
            updateSql.append(condition);
            String sql = updateSql.toString();
            logger.debug(sql);
            return update(sql, args);
        }
    }

    /**
     * 获取查询的属性
     *
     * @return
     */
    private String buildSelectStatement() {
        StringBuilder statement = new StringBuilder();
        List<FieldDescriptor> fields = descriptor.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i == fields.size() - 1) {
                statement.append(fields.get(i).getName());
            } else {
                statement.append(fields.get(i).getName()).append(",");
            }
        }
        return statement.toString();
    }

    /**
     * get Builder from messageClass
     *
     * @param messageClass
     * @return
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    private <T extends Message> T.Builder newBuilder(Class<T> messageClass) {
        T.Builder builder = null;
        try {
            builder = (T.Builder) MethodUtils.invokeStaticMethod(messageClass, "newBuilder");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new BeanCreationException("Error in create Message instance, " + messageClass + ".", e);
        }
        return builder;
    }
}
