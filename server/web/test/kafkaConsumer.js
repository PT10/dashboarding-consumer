const Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'test12',
  'metadata.broker.list': '192.168.33.15:9090', //192.168.33.15:9090
  'enable.auto.commit': false
}, { 'auto.offset.reset' : 'earliest'});

var timeout = 1000; // 10 seconds
consumer.connect();

var from_offset = 499;
var to_offset = 499;
var visited = 0;
var fromTimestamp = 1199113200000; //1199133000000;
var toTimestamp = 1199145180000; //1199140200000;
var msgCount=0;
var totalCount = 0;
var currentCount = 0;
var resume = true;

 consumer
  .on('ready', async function() {

    consumer.offsetsForTimes(
        [ {topic: 'demo', partition: 0, offset: toTimestamp } ], //1600704585000 //1600924335000 //stormEventsSmall-v0
        timeout,
        countOffset);
    
    while(visited!=1) {
            await new Promise(r => setTimeout(r, 200));
    }


    //const runConsumer = async () => { await 
    consumer.offsetsForTimes(
        [ {topic: 'demo', partition: 0, offset: fromTimestamp } ], //1600704585000 //1600924335000 //stormEventsSmall-v0
        timeout,
        offsetCallback);
    //}

    //await runConsumer();

    while(visited!=2) {
        await new Promise(r => setTimeout(r, 200));
    }

    console.log("assigning offset : " + from_offset)
    await consumer.assign([{
        topic: 'demo',
        partition: 0,
        offset: from_offset,
    }])

    //have to add sleep here, otherwise consume doesn't work
    //await new Promise(r => setTimeout(r, 5000));

    totalCount =  parseInt(to_offset) - parseInt(from_offset)
    console.log("Fetching " + totalCount + " records")

     while (currentCount < totalCount) {
             consumer.consume(totalCount, callbackConsume);
             console.log("current count is " + currentCount);
             await new Promise(r => setTimeout(r, 100));
     }
    //await consumer.consume(); //5,callbackConsume);

    /*while(msgCount<to_offset-from_offset) {
        console.log("msgCount is " + msgCount);
        await new Promise(r => setTimeout(r, 500));
    }*/

    consumer.unassign();
    console.log("current count is " + currentCount);
    console.log("Disconnecting ")
    consumer.disconnect();

})

/*consumer.on('data', function(data) {
    // Output the actual message contents
    console.log("Inside getData");
    //console.log(data);
    console.log(data.offset + ":" + data.timestamp);
    msgCount++;
    if(+data.timestamp>=toTimestamp) {
        console.log("Disconnecting");
        //consumer.disconnect();
        //setTimeout(function() {
          //  consumer.disconnect();
          //}, 100);
          
          
    }
  })*/
      ;


function countOffset(err, offsetMap){
        //obj2 is array of map (offset, parition, topic)
        //console.log(obj1);
        console.log("to offset is " + offsetMap[0]["offset"]);
        to_offset = +offsetMap[0]["offset"]
        visited = 1;
    }
    

function offsetCallback(err, offsetMap){
    //obj2 is array of map (offset, parition, topic)
    //console.log(obj1);
    console.log("from offset is : " + offsetMap[0]["offset"]);
    from_offset = +offsetMap[0]["offset"]
    visited = 2;
    }


function callbackConsume(err, messages) {
        console.log("In consumerCallback");
        currentCount += messages.length;
        
        for(i=0;i<messages.length;i++){
            //if(+messages[i].timestamp<toTimestamp) {
            //msgCount++;
            console.log(messages[i].offset);
            //}
            //console.log(messages[i].value.toString()); //.value.toString());
          //  console.log(messages[i].offset); //.value.toString());
        }
      
      }
