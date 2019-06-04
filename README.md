# 交通拥堵预测

**思路**：考虑到没有相应的数据来源，这里是数据使用kafa, 进行自主模拟生产，然后消费预处理后存储到非关系型数据库中，再从redis中读取数据进行数据建模，将建立的模型存储到hdfs上，等进行数据预测时读取hdfs上的预测模型，进行预测。

**项目环境**

| 操作系统 | 版本号         |
| -------- | -------------- |
| window   | window10家庭版 |
| Linux    | Ubuntu18.04    |

**大数据框架环境**

| 框架      | 版本号             |
| --------- | ------------------ |
| hadoop    | Apache 2.7.2       |
| zookeeper | Apache 3.4.5       |
| kafka     | kafka_2.10_0.8.2.1 |
| scala     | Apache 2.1.1       |
| spark     | 3.0.5              |
| redis     | 2.11.8             |
| jdk       | 1.8                |

| 编程工具      | 版本号   |
| ------------- | -------- |
| Intellij IDEA | 2019.1.3 |

**数据结构**

| 字段名     | 字段含义                                 | example     |
| ---------- | ---------------------------------------- | ----------- |
| monitor_id | 监测点id，在同一个业务系统中是唯一的     | 0001、002等 |
| speed      | 当前某一辆车经过监测点的时速，单位：km/h | 060、010等  |

**启动大数据集群环境**

注意：启动大数据集群环境，这里因为资源有限，我以单节点作为测试，此次启动命令都是绝对启动命令，都是绝对路径，请根据自己实际情况进行启动。

```
1、启动 hadoop

$ /usr/local/hadoop/sbin/start-all.sh


启动 kafka   http://dblab.xmu.edu.cn/blog/1096-2/#more-1096

$ /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties

$ /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties

2、创建主题

$ /usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --topic traffic --create --replication-factor 1 --partitions 1

创建的主题是否存在。

$ /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181  

测试启动成功

手动生产消息
$ /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic traffic

接收
$ /usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic traffic --from-beginning


启动redis

$ /usr/local/redis/src/redis-server /usr/local/redis/redis.conf

# 测试启动
$ /usr/local/redis/src/redis-cli -h 192.168.126.132

```

关闭集群

```
关闭zookeeper
$ /usr/local/kafka/bin/zookeeper-server-stop.sh

关闭kafka
$ /usr/local/kafka/bin/kafka-server-stop.sh

关闭hadoop
$ /usr/local/hadoop/sbin/stop-all.sh

关闭redis
$ /usr/local/redis/src/redis-cli -h 192.168.126.132 -p 6379 shutdown
```





### 编写代码--创建总工程--traffic_prediction

- 创建普通Java工程：traffic_prediction
- 删除 src 目录（工程traffic_prediction 作为总工程目录，不编写代码）

### 编写生产者

**思路：**

**a**)新建子模块工程：tf_produccer

- New ——> Module ——>选择maven 工程——> 创建完毕——>右键Add Framework Support 添加scala支持
- 创建scala目录并设置为Source Root

**b)** 配置maven依赖

**c)** 因为要把数据发送给kafka，所以配置kafka属性，保存于某个配置文件中

**d)** 编写kafka加载属性的工具类

**e)** 每隔5分钟，切换一次模拟状态，例如第一个五分钟，车速都在30KM/H以上，下一个五分钟，车速都在10KM/H一下，往复模拟公路一会堵车，一会不堵车的情况。

**f)** 启动zookeeper，kafka，并创建kafka主题，检查主题存在性

**g)** 将数据发送至kafka并使用kafka console-consumer进行检测



### **编写消费者**

**思路：**

**a**)新建子工程：tf_consumer

**b)** 配置maven依赖

**c**) 配置redis并测试

**d**) 将刚才kafka.properties以及PropertyUtil拷贝过来

**e**) 编写redis操作工具类：RedisUtil

**f**) 读取kafka中的数据，实时保存到redis中，并且按照分钟和监测点聚合车速和车辆个数



### 编写数据建模代码

**思路：**

**a)** 确定要对哪个监测点进行建模，我们称之为目标监测点

**b**) 找到目标监测点的其他相关监测点（比如相关监测点与目标监测点属于一条公路的）

**c**) 从redis中访问得到以上所有监测点若干小时内的历史数据信息（一部分作为训练数据，一部分作为测试数据）

**d**) 提取组装特征向量与目标向量，训练参数集，训练模型

**e**) 测试模型吻合度，将符合吻合度的模型保存到HDFS中，同时将模型的保存路径放置于redis中

### 编写预测模块

**思路：**

**a)** 用户传入想要进行预测的时间节点，读取该时间节点之前3分钟，2分钟和1分钟的数据

**b**)此时应该已经得到了历史数据集，通过该历史数据集预测传入时间点的车流状态

**提示**：为了方便观察测试，建议传一个历史时间点，这样可以很直观的看到预测结果是否符合期望值。



**警告：代码中涉及到的IP，请自觉更换为你所使用的的集群的IP，我的单机的是192.168.126.132**





















































