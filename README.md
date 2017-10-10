# Charlatan

Charlatan is a library created for Apache Kafka in order to substitute real Zookeeper with Zookeeper imitation.
Charlatan was tested only with Apache Kafka version 0.11.0.1. 

## Motivation
Kafka is a great product, but it requires running Zookeeper cluster. This means that to manage 
Kafka cluster we also need to manage Zookeeper cluster.  
This complicates Kafka usage especially in case when we already have a service similar to Zookeeper. 
For example etcd, consul, etc. or your own highly available service.

Charlatan provides two options to use Kafka without Zookeeper:
1. Instead of Zookeeper Kafka communicates with the different server using Zookeeper messaging 
protocol. This option was implemented in [***Charlatan-server***](#charlatan-server) sub-project.
2. Instead of Zookeeper driver Kafka uses Charlatan adapter that stores all Zookeeper related
information in the relational database. In this case Kafka doesn't need other running service
and database can play the role of the highly available service. This option was implemented in 
[***Charlatan-adapter***](#charlatan-adapter) sub-project.

## Sub-projects 
Charlatan contains from few sub-projects

Sub-project             |Description
------------------------|------------ 
[***Charlatan-common***](#charlatan-common)    |Watch management, session management, common dao interfaces.
[***Charlatan-server***](#charlatan-server)    |Netty based server implementation, Zookeeper messaging protocol implementation.
[***Charlatan-adapter***](#charlatan-adapter)  |Fake Zookeeper driver, it can be used as a Kafka dependency instead of Zookeeper.jar.
[***Charlatan-sqlite***](#charlatan-dao-sqlite)|Simple implementation of Charlatan DAO interfaces using sqlite. It is not  suitable for the production usage and it was build mainly for the demonstration and testing purposes.
[***Charlatan-remote***](#charlatan-dao-remote)|Simple netty based implementation of Charlatan DAO interfaces that sends DAO requests to the remote server.

### Charlatan-common<a name="charlatan-common"></a>
In order to replace Zookeeper Charlatan library has to handle all the Zookeeper features. 

Apache Kafka uses Zookeeper mainly to:
* Keep specific Kafka related information: which topics exist, how many partitions topic has, 
topic replication information, etc. This kind of information is stored in a node tree 
(check Zookeeper documentation for details).
* Track status of Kafka brokers: who is the cluster leader, which broker is alive, 
which broker is leaving/joining.
* Support watches. Zookeeper has concept of watches: a watch event is one-time trigger, sent to the 
client that set the watch, which occurs when the data for which the watch was set changes. 
In practice this means that if Kafka broker wants to receive an update in case specific node has changed
or children node changes, it sets a watch for node in question.
                  
All these features are implemented in the ***Charlatan-common***. 
* In order to keep Kafka related information Charlatan supports nodes CRUD operations by providing
NodeDao interface that needs to be implemented for your database or other highly available service.
* In order to track broker statuses  ***Charlatan-common*** provides its own implementation
[SessionServiceImpl](charlatan-common/src/main/java/com/pega/charlatan/server/session/service/SessionServiceImpl.java) 
To use custom implementation [SessionService](charlatan-common/src/main/java/com/pega/charlatan/server/session/service/SessionService.java) 
interface must be implemented.
* Watches support is implemented in [WatchServiceImpl](charlatan-common/src/main/java/com/pega/charlatan/watches/service/WatchServiceImpl.java).
To use custom implementation [WatchService](charlatan-common/src/main/java/com/pega/charlatan/watches/service/WatchService.java) 
interface must be implemented.
   
### Charlatan-server<a name="charlatan-server"></a>
This option implies that standard Kafka distribution is used and instead of real Zookeeper
Kafka is pointed to the Charlatan server. The schema could look like: 
![](images/CharlatanServer.png "Charlatan server usage")
Note that as long as same database is used multiple Charlatan server can be running.
One or multiple Kafka instances can be connected to the Charlatan server. 
Kafka broker isn't aware that it doesn't connect to a real Zookeeper. 
Standard Kafka setting ```Zookeeper.connect``` is used in order to point Kafka to Charlatan server.  

In this example we are starting Charlatan server with sqlite DAO implementations 
and default implementation of WatchService and SessionService.
```java
String host = "localhost";
int port = 2181;

CharlatanNettyServer server = new CharlatanServerBuilder()
	// Charlatan server host
	.setHost(host)
	// Charlatan server port
	.setPort(port)
	// Charlatan server id must be unique per Charlatan cluster
	.setId("server"+port)
	.setWorkerCount(5)
	// Node DAO implementation
	.setNodeDao(new NodeDaoSqlite())
	// Watch service implementation
	.setWatchService(new WatchServiceImpl(NodeUpdateDaoSqlite(), new NamedThreadFactory("charlatan-watch-service")))
	// Session service implementation
	.setSessionService(new SessionServiceImpl(new SessionDaoSqlite()))
	.setThreadFactory(new NamedThreadFactory("charlatan-service"))
	.build();

server.start();
```

### Charlatan-adapter<a name="charlatan-adapter"></a>
This option implies that in standard Kafka distribution Zookeeper.jar library will be replaced
by the charlatan-adapter.jar and charlatan dao implementation will be provided. 
What do we need to make it work?
1. Remove from Kafka distribution Zookeeper jar located in libs/Zookeeper-3.4.10.jar.
2. Place charlatan-adapter.jar in libs/ folder.
3. Place jar that contains your Charlatan DAO implementations in libs/ folder (charlatan-dao-sqlite.jar can be 
used for testing purposes).
<br>In runtime ***Charlatan-adapter*** uses DAO interface implementations found in the classpath. 
***Charlatan-adapter*** fails if no DAO implementation was found or multiple DAO implementations were found.

In case Charlatan DAO interfaces were implemented for relational database the schema would look like:

![](images/CharlatanAdapterDB.png "Charlatan adapter with relational database")

In a more general approach Charlatan DAO interfaces implementation is a client to your 
highly available service.

![](images/CharlatanAdapter.png "Charlatan adapter general implementation")

### Charlatan-dao-sqlite<a name="charlatan-dao-sqlite"></a>
This project was created for testing purposes and contains basic Charlatan DAO implementations for
sqlite database.
### Charlatan-dao-remote<a name="charlatan-dao-remote"></a>
This project was created for demonstration purposes only and contains Charlatan DAO implementation 
that acts as a client to the remote REST-ful service. 
