# simple-beam-jmsio
Simple sample implementing Apache Beam JmsIO

# How to use
Adapt init-java-home.sh
```
./init-java-home.sh
gradle clean build
java -jar build/libs/simple-beam-jmsio-1.0.0-SNAPSHOT.jar
```

Publish messages:
```
./sdkperf_jmsamqp.sh -cip="amqp://localhost:5672" -cu=default@default -cp=xxx -pql="SOLACE_BEAM_READ" -mn=1000 -mr=100 -pfl=message.json
```
