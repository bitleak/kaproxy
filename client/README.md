# kaproxy client

## install

```shell
go get github.com/meitu/kaproxy
```

## Usage
```golang
import github.com/meitu/kaproxy/client

c := client.NewKaproxyClient(host, port, token)

c = c.WithTimeout(5 * time.Second) //optional, return a pointer to kaproxyClient which http client's timeout is specified.  

```

### Producer

```golang
//produce a message, key and value is []byte
msg := client.Message{key, value}

resp, err := c.Produce(topis, msg)
if err !=nil {
    panic(err)
}

// produce a message, message will be hashed to the partition according to the key
resp, err = c.ProduceWithHash(topic, msg)

// produce to specified partition
resp, err = c.ProduceWithPartition(topic, msg)
```

### Consumer

```golang
//AtMostOnce semantic
blockingTimeout := 3 * time.Second
resp, err := c.Consume(group, topic, blockingTimeout)

//AtLeastOnce semantic. 
//After the user receives the message, if kaproxy does not receive the ack, when the user sends a consume request after ttr, the message will be replied to the user again.
//This usage for atLesastOnce group only

ttr := 30 * time.Second
resp, err := c.Consume(group, topic, blockingTimeout, ttr)
if err != nil {
    panic(err)
}

err = c.ACK(group, topic, resp)
```
