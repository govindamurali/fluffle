# Fluffle
RabbitMQ library which abstracts the complications of queue creation, subsciption, channel maintenance and connection management.
Opens connections on demand with optimal use of channels with an internal connection pool, limit optionally configurable per channel


## Install
	go get github.com/govindamurali/fluffle


## Usage
#### 1. Start the service
```
  fluffle.Start(rabbitMQConfig, logger)	
```

#### 2. Create Queues
```
  sampleQueue:= fluffle.New("sample queue", prefetchCount)
```

#### 3. Publish
```
  err:= sampleQueue.Publish(someData)
  err = sampleQueue.PublishIdempotent(idempotencyKey, idempotencyValue, someData)
```

#### 4. Consume
```
  for msg := range sampleQueue.Consume() {
      err := json.Unmarshal(msg.Body, &customStruct)
      // do your thing
    }
```



