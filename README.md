 

## 使用场景

该应用主要用于指标聚合计算输出场景，支持自定义聚合分组、聚合字段等规则，规则支持热更新，即任务不需要重启规则就可以生效。

## 事件数据

目前仅支持influxdb的数据格式，示例数据如下

```java
esb_as_ProcBizFuncLibs,func_caption=LS_账户同步_交易数据获取,func_no=160501,host=18.18.1.101,monitorKey=abb73c5de7344cf2a48e84073d6b35ea ave_time=15.0,createtime=1623287368888.0,cur_avgExcCount=0.0,cur_avgTime=0.0,cur_excCount=0.0,cur_excTime=0.0,cur_que_ave_time="0",curr_queue_ave_time="0",max_time=23.0,min_time=7.0,time_see=224.0,total=15.0 1623287369286000016
```



## 规则数据

提前准备规则数据到Mysql，建表语句：

```sql
CREATE TABLE `rules` (
  `id` int(11) NOT NULL comment '规则ID',
  `table_name` varchar(100) NOT NULL COMMENT '表名称',
  `rule_state` varchar(10) DEFAULT NULL comment '规则状态：ACTIVE-启用  PAUSE-禁用   DELETE-删除',
  `grouping_key_names` varchar(100) DEFAULT NULL comment '分组字段',
  `agg_fields` varchar(1000) DEFAULT NULL comment '聚合字段和类型，类型包括SUM/MAX/MIN等',
  `window_minutes` bigint(20) DEFAULT NULL comment '窗口长度，单位为分钟',
  PRIMARY KEY (`id`)
)
```

| 名称               | 类型                                         | 描述                                            |
| ------------------ | -------------------------------------------- | ----------------------------------------------- |
| id                 | Integer                                      | 规则ID                                          |
| table_name         | String                                       | 规则生效的表名称                                |
| rule_state         | RuleState                                    | 规则状态：ACTIVE-启用  PAUSE-禁用   DELETE-删除 |
| grouping_key_names | List<String>                                 | 分组字段                                        |
| agg_fields         | List<Tuple2<String, AggregatorFunctionType>> | 聚合字段和类型，类型包括SUM/MAX/MIN/AVG/COUNT等 |
| window_minutes     | Integer                                      | 窗口长度，单位为分钟                            |

规则示例数据

| ruleId | table_name | rule_state | grouping_key_names | agg_fields                       | window_minutes |
| ------ | ---------- | ---------- | ------------------ | -------------------------------- | -------------- |
| 1      | table1     | active     | host&monitorKey    | field1:sum&field1:max&field1:min | 10             |
| 2      | table2     | delete     | host&monitorKey    | field1:sum&field1:max&field1:min | 10             |
| 3      | table3     | active     | host&monitorKey    | field2:avg&field2:count          | 60             |

## 数据打宽

支持对主流数据进行打宽操作(维表关联)，如根据主机IP扩展归属部门、责任人等字段

indicator.properties配置文件新增如下配置

```shell
# lookup配置，支持lookup方式的维表关联进行打宽操作
# 是否开启
lookup.enable=true
# 维表database名称
lookup.database.name=test
# 维表表名称
lookup.table.name=host_department_Info
# 关联的key，左key=右key，多个以逗号分隔
lookup.table.join-keys=host=host,monitorKey=monitor_key
# 关联需要查询的字段
lookup.table.extend-fields=owner,department_name
# 缓存最大记录数
lookup.cache.max-size=1000
# 缓存过期时间
lookup.cache.expire-ms=10000
```



## 启动脚本

```shell
需要指定FLINK_HOME
.bin/indicator-job.sh start
```



```shell
# indicator.properties配置文件

# 参数说明
# 事件流数据源支持kafka/socket
data.source=kafka 
# 时间流topic名称
data.topic=liveindicators
# 规则流数据源支持mysql
rules.source=mysql 
# 规则存储数据库名称
rules.database.name=test
# 规则存储数据库表名
rules.table.name=rules
# 结果输出 支持elasticsearch/STDOUT
results.sink=elasticsearch 
# kafka地址
kafka.host=10.20.30.113 
# kafka端口
kafka.port=9092 
# es地址
elasticsearch.host=10.20.30.112 
# es端口
elasticsearch.port=9200
# mysql地址
mysql.host=10.20.30.113
# mysql端口
mysql.port=33061
# mysql用户名
mysql.username=root
# mysql密码
mysql.password=admin@123
```

## 结果查询

默认输出Elasticsearch，查询语句如下：

测试kibana地址`http://10.20.30.112:5601/app/dev_tools#/console`

```json
GET esb_as_procbizfunclibs/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "monitorKey": "246181c7b2e4401bb52b0e30eb7fa56f"
          }
        },
        {
          "match": {
            "host": "18.18.1.113"
          }
        }
      ]
    }
  }
}
```

```json
// result
      {
        "_index" : "esb_as_procbizfunclibs",
        "_type" : "indicator",
        "_id" : "{18.18.1.113-246181c7b2e4401bb52b0e30eb7fa56f-1623283800000-1623284400000}",
        "_score" : 5.496153,
        "_source" : {
          "host" : "18.18.1.113",
          "monitorKey" : "246181c7b2e4401bb52b0e30eb7fa56f",
          "startTime" : "2021-06-10 08:10:00:000",
          "endTime" : "2021-06-10 08:20:00:000",
          "cur_excCount-MAX" : 8.0,
          "cur_excCount-MIN" : 7.0,
          "cur_excCount-AVG" : 7.5714,
          "cur_excCount-COUNT" : 7,
          "cur_excCount-SUM" : 53.0
        }
      }
```

