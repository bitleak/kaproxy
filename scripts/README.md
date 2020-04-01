## Setup dev env

For mac user, install [docker for mac](https://docs.docker.com/docker-for-mac/) first.

Make sure `docker` and `docker-compose` are reachable. 

In order to simulate two datacenter environment, we would bring up two kafka clusters.

Run:
```
cd ${SCRIPT_DIR}/docker-datacenter-1/
./setup.sh

cd ${SCRIPT_DIR}/docker-datacenter-2/
./setup.sh
```

`setup.sh` would start the kafka cluster of one single broker, and creates a default
topic of one partition (replication-factor = 1).

### datacenter-1 info

- kafka broker = 127.0.0.1:9092
- zookeeper = 127.0.0.1:2181
- token = test
- group = pull-test
- topic = test
- adminID = admin

### datacenter-2 info

- kafka broker = 127.0.0.1:9093
- zookeeper = 127.0.0.1:2182
- token = test
- group = g1
- topic = test
- adminID = admin


### Teardown dev env

To cleanup, run:

```
cd ${SCRIPT_DIR}/docker-datacenter-1/
./teardown.sh

cd ${SCRIPT_DIR}/docker-datacenter-2/
./teardown.sh
```


## Benchmark dev env

- `bench-consume.sh`: consume message from kaproxy using redis proto
- `bench-produce.sh`: produce message to kaproxy using http


## Notes
the components' version:
- kafka = 0.8.2.2
- zookeeper = latest
