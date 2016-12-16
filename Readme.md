FluentGO
--------

FluentGO is a replacement of favourite FluentD. It has the same aims, such as
log and data collection. The main difference is the performance, in addition,
memory consumption.

The project started with a problem that was occurring in FluentD’s Redis input
plugin. By design, Redis disconnects a PubSub clients if clients are not fast
enough to process coming messages ([Redis Client
Handling](https://redis.io/topics/clients)). While this is the default nature of
the Redis PubSub system, FluentD is unable to reconnect to the related channel
again without restarting the process. None of the many configurations, both on
Redis and FluentD, worked it gave a start to the project.

Now, besides its many input and output handlers, it has many tuning
configurations, which makes it a suitable solution as a fast, reliable
production ready log collection system.

Possible inputs:
* Redis Pub/Sub
* Redis List
* Amazon SQS
* Amazon Kinesis
* RabbitMQ
* Apache Kafka
* TCP
* UDP

Possible outputs:
* Console Out
* Null, for testing
* Amazon S3
* Amazon SQS
* Amazon Kinesis
* ElasticSearch
* Redis Pub/Sub
* Redis List
* RabbitMQ
* Apache Kafka
* Mongo
* TCP
* UDP
* File
