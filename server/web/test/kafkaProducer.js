
const Kafka = require('node-rdkafka');
const csv = require('csv-parser');
const fs = require('fs')

var producer = new Kafka.Producer({
    'metadata.broker.list': 'localhost:9092', //192.168.33.15:9090
    'dr_cb': true
  });

  // Connect to the broker manually
  producer.connect();
  
  producer.on('ready', function() {


    //delete after this

    
    /*try {

      message = "{\"StartTime\":\"2007-12-31T23:53:00Z\",\"EndTime\":\"2007-12-31T23:53:00Z\",\"EpisodeId\":\"12037\",\"EventId\":\"65838\",\"State\":\"CALIFORNIA\",\"EventType\":\"High Wind\",\"InjuriesDirect\":\"0\",\"InjuriesIndirect\":\"0\",\"DeathsDirect\":\"0\",\"DeathsIndirect\":\"0\",\"DamageProperty\":\"0\",\"DamageCrops\":\"0\",\"Source\":\"Mesonet\",\"BeginLocation\":\"\",\"EndLocation\":\"\",\"BeginLat\":\"\",\"BeginLon\":\"\",\"EndLat\":\"\",\"EndLon\":\"\",\"EpisodeNarrative\":\"One last round of offshore winds affected sections of Southern California. Unlike previous events during December, the offshore winds with this event were confined to the mountains of Ventura and Los Angeles counties.\",\"EventNarrative\":\"The Warm Springs RAWS sensor reported northerly winds gusting to 58 mph.\",\"StormSummary\":\"{\\\"TotalDamages\\\":0,\\\"StartTime\\\":\\\"2007-12-31T23:53:00.0000000Z\\\",\\\"EndTime\\\":\\\"2007-12-31T23:53:00.0000000Z\\\",\\\"Details\\\":{\\\"Description\\\":\\\"The Warm Springs RAWS sensor reported northerly winds gusting to 58 mph.\\\",\\\"Location\\\":\\\"CALIFORNIA\\\"}}\"}";

      console.log("writing to kafka");
      producer.produce(
        'stormEventsSmall-v0',
        0,
        Buffer.from(message),
        'Bolt',
        1199145180000,
      );
    } catch (err) {
      console.error('A problem occurred when sending our message');
      console.error(err);
    }
    console.log("writing done");
    producer.on('event.error', function(err) {
      console.error('Error from producer');
      console.error(err);
    });
    
    producer.setPollInterval(100);
  });*/



fs.createReadStream('/Users/abhishek/git/Bolt/log_poc/StormEventsSmall.csv')
  .pipe(csv(["StartTime","EndTime","EpisodeId","EventId","State","EventType","InjuriesDirect","InjuriesIndirect","DeathsDirect","DeathsIndirect","DamageProperty","DamageCrops","Source","BeginLocation","EndLocation","BeginLat","BeginLon","EndLat","EndLon","EpisodeNarrative","EventNarrative","StormSummary"]))
  .on('data', (row) => {  

    //console.log("INPUT: " + JSON.stringify(row));
    startDate = new Date(Date.parse(row['StartTime']));
    if(!startDate || startDate==NaN)
       return;
    
    if(startDate.getTime()>=1198666800000) {
      console.log("Dumping mesage  for " + startDate.getTime());
    try {
    producer.produce(
      'demo', //'stormEventsSmall-v0',
      0,
      Buffer.from(JSON.stringify(row)),
      'Bolt',
      startDate.getTime(),
    );
  } catch (err) {
    console.error('A problem occurred when sending our message');
    console.error(err);
  }}
}).on('end', () => {
    
});


    // 2007-01-01T00:00:00Z
    // debug_log(Date.parse(row['StartTime']));
    //row['StartTime'] = Date.parse(row['StartTime']);
    //row['EndTime'] = Date.parse(row['EndTime']);
    
    
  })
  
  producer.on('event.error', function(err) {
    console.error('Error from producer');
    console.error(err);
  });
  
  producer.setPollInterval(100);


