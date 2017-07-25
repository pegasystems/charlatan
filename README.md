# zookeeper-fake

zookeeper-fake is a library created for Apache Kafka in order to
substitute real zookeeper with zookeeper imitation. Zookeeper-fake stores
all zookeeper related information in the relational database and it is
part of the kafka node.
With zookeeper-fake Kafka node/cluster doesn't need zookeeper cluster
and works on its own.

## Sub-projects
zookeeper-fake consists from two sub-projects

### zookeeper-db
zookeeper-db contains all logic and dao interfaces. In order to use the
specific database with zookeeper-db, dao interfaces have to be implemented.
Atm zookeeper-fake provides only sqlite implementation that is located
in zookeeper-db-sqlite sub-project.

In runtime zookeeper-db uses dao implementations found in the classpath.
zookeeper-db fails if no dao implementation was found or multiple dao
implementations were found.

### zookeeper-db-sqlite
Sqlite implementation of dao interfaces. It is assumed that
**sqlite.properties** file is placed in the root of the project

    database=<path_to_the_db>.db