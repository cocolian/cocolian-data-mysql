## cocolian-data-mysql

实现将Protobuf的 message 持久化到MySQL数据库中的功能。 

Cocolian将主要使用Protobuf来描述数据对象， 为此有必要提供一个支持ProtobufMessage 持久化的中间件， 来简化开发工作。 

## 定义

这个模块实现理念同Hibernate， 基于标签（protobuf 的option)来完成对持久化设置的描述，并通过持久化引擎来支持最终的实现。 

对于一个message的定义，对应的数据表的持久化选项：

```java
message TableOption {
	optional string name = 1; // 表名， 默认同message的类名。
	optional string name_pattern = 2; //分表分库时的表名pattern;
	optional string primary_key = 3; //主键字段， 默认为空
}
```

对于message下的field定义，对应的列的持久化选项：

```java
message ColumnOption {
	optional string name = 1; //列名，默认为field的name
	optional string type = 2; //列类型，默认为field的type
}
```

使用这两个option来定义一个message的持久化信息，例子如下：


```java
message Foo {
	option (table_option).name = "foo_table" ; //定义Foo存储对应的table名称，如果没有，则默认使用 foo作为表名 。 
	option (table_option).primary_key = "id" ; //主键 
	required int32 id 		= 1 [(colomn_option).name = "id", (column_option).type = "int32"]; //定义列名和数据类型，默认为字段名和字段类型。 
	optional int32 bar1 	= 2 [(colomn_option).name = "bar-1" ]; //定义列名和数据类型，默认为字段名和字段类型。 
	optional string bar2 	= 3 [(column_option).type = "varchar(32)"]; //定义列名和数据类型，默认为字段名和字段类型。 
}
```

## 使用

实现了JdbcMessageTemplate之后，对Foo的持久化操作如下：

```java
JdbcMessageTemplate<Foo> template = new JdbcMessageTemplate<Foo>(...);
Foo.Builder foo = Foo.newBuilder();
foo.setId(12);
... //设置foo的其他属性；

template.insert(foo.build()); //创建；

Foo.Builder foo = template.get(12).toBuilder(); //根据id来读取对象； 
foo.setXXX(); //设置属性； 
template.update(foo.build()); // 全量更新； 

template.partialUpdate(foo.build()); //仅更新foo中有设置值的部分； 

template.remove(foo.getId()); //删除；

```

## 技术栈

- Google Protobuf : 核心数据表示
- Spring JdbcTemplate: 使用spring 的这个类来操作数据库。 
