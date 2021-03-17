
const Kafka = require('node-rdkafka');
const topic = 'demo'
const kafkaServer = '192.168.33.15:9090'

var consumer = new Kafka.KafkaConsumer({
  'group.id': 't',
  'metadata.broker.list': kafkaServer,
  'auto.offset.reset': 'earliest'
}, {});

var timeout = 1000; // 10 seconds
var fromTimestamp = 0;
var toTimestamp = 0;

var fromOffsetMap = []
var toOffsetMap = []

consumer.connect(undefined, function(){
  consumer.offsetsForTimes(
      [ {topic: topic, partition: 0, offset:  toTimestamp} ], //1600704585000 //1600924335000
      timeout,
      countOffsetCallback);
  
  consumer.offsetsForTimes(
  [ {topic: topic, partition: 0, offset:  fromTimestamp} ], //1600704585000 //1600924335000
  timeout,
  offsetCallback);

  consumer.disconnect();
});

function consumerCallback(err, messages) {
    console.log("In consumerCallback");
    for(i=0;i<messages.length;i++)
        console.log(messages[i].value.toString()); //.value.toString());

}

function countOffsetCallback(obj1, obj2) {
    toOffsetMap = obj2;
    console.log(toOffsetMap);
}

function offsetCallback(obj1, obj2){
  //obj2 is array of map (offset, parition, topic)
  console.log(obj2);
  var consumer = new Kafka.KafkaConsumer({
    'group.id': 't',
    'metadata.broker.list': kafkaServer,
    //'auto.offset.reset': 'earliest'
  }, {});
  // var timeout = 1000; // 10 seconds
  consumer.connect(undefined, function(){
    consumer.assign(obj2);
    //console.log("Calling consume()");
    consumer.consume(2022, consumerCallback);
  });
}
