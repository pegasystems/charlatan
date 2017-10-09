# Zooikeeper

Zooikeeper (from Dutch _zooi_ - mess, chaos) is a library created for Apache Kafka in order to
substitute real zookeeper with zookeeper imitation. Zooikeeper stores
all zookeeper related information in the relational database and it is
part of the kafka node.
With zooikeeper Kafka node/cluster doesn't need zookeeper cluster
and works on its own.