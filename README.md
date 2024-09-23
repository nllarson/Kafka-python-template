## Confluent Cloud - Sample Python Producer / Consumer

Based on examples pulled from [confluent-kafka-pyton](https://github.com/confluentinc/confluent-kafka-python).

### Usage
Set client configurations in `client.properties`.  This can be run for OAuth and Basic Auth clients.  Be sure to comment out the set of configs you aren't using.

Also add Schema Registry cluster / user information in `sr-client.properties`.

By default, the script will produce to a `user` topic backed with a schema that matches `/avro/user-v1.avsc`.

### Install

``` bash
$ pip install -r requirements.txt
```

