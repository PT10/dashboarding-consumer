// Kafka
/*
exports.conf = { 
    "StormEvents" : { 
        "type" : "kafka",  
        "handle" : "localhost:9092#demo3" , 
        "schema" : { 
            "StartTime" : "datetime",
            "EpisodeNarrative": "string",
            "State": "string"
        } 
    } 
}
*/


// File
/*exports.conf = { 
    "Databases":{
      "sonicwall":{
        "Name":"sonicwall",
        "Functions":{},
        "Tables":{
          "index":{
            "Name":"index",
            "Type":"file",
            "Handle":"/Users/prasad/Downloads/3m_small.log",
            "OrderedColumns":[{"Name":"line","Type":"System.String","CslType":"string"}]},
            "aggregate":{
              "Name":"aggregate",
              "Type":"file",
              "Handle":"localhost#8983#aggregate#",
              "OrderedColumns":[
                {"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},
                {"Name":"sent_mb","Type":"System.Real","CslType":"real"},
                {"Name":"rcvd_mb","Type":"System.Real","CslType":"real"}]},
              "stream":{
                "Name":"stream",
                "Type":"kafka",
                "Handle":{"kafkaUrl":"bolt-node:9092","topic":"syslogingest2","groupId":"","numPartitions":8},
                "OrderedColumns":[
                  {"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},
                  {"Name":"message","Type":"System.String","CslType":"string"}]},
                "lan_wan_traffic":{
                  "Name":"lan_wan_traffic",
                  "Type":"kafka",
                  "Handle":{"kafkaUrl":"bolt-node:9092","topic":"bolt_inf_Sent_Rcvd_MB","groupId":"","numPartitions":8,"processingBatchSize":1,"avroSpec":{"type":"record","name":"ContentArray","fields":[{"name":"contents","type":{"type":"array","items":{"type":"record","name":"AnomalyResults","fields":[{"name":"partition_fields","type":{"type":"array","items":"string"}},{"name":"aggr_field","type":"string"},{"name":"jobId","type":"string"},{"name":"id","type":"string"},{"name":"timestamp","type":"string"},{"name":"actual_value","type":"float"},{"name":"expected_value","type":"float"},{"name":"is_anomaly","type":"boolean"},{"name":"score_value","type":"float"},{"name":"method","type":"string"},{"name":"Severity","type":"string"},{"name":"alerts_severity","type":"string"},{"name":"forecasted_values_fs","type":{"type":"array","items":"float"}}]}}}]},"avroField":"contents"},"OrderedColumns":[{"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},{"Name":"aggr_field","Type":"System.String","CslType":"string"},{"Name":"actual_value","Type":"System.Real","CslType":"real"},{"Name":"expected_value","Type":"System.Real","CslType":"real"},{"Name":"score_value","Type":"System.Real","CslType":"real"},{"Name":"Severity","Type":"System.String","CslType":"string"}]}}},"sonicwall_ingest":{"Name":"sonicwall_ingest","Functions":{},"Tables":{"index":{"Name":"index","Type":"shardedsolr","Handle":"localhost#8983#sonicwall#6","OrderedColumns":[{"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},{"Name":"message","Type":"System.String","CslType":"string"}]},"aggregate":{"Name":"aggregate","Type":"shardedsolr","Handle":"localhost#8983#aggregate#","OrderedColumns":[{"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},{"Name":"sent_mb","Type":"System.Real","CslType":"real"},{"Name":"rcvd_mb","Type":"System.Real","CslType":"real"}]},"stream":{"Name":"stream","Type":"kafka","Handle":{"kafkaUrl":"bolt-node:9092","topic":"syslogingest2","groupId":"solringest","numPartitions":8},"OrderedColumns":[{"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},{"Name":"message","Type":"System.String","CslType":"string"}]},"stream_lan_wan":{"Name":"stream_lan_wan","Type":"kafka","Handle":{"kafkaUrl":"bolt-node:9092","topic":"syslogingest2","groupId":"lan_wan","numPartitions":8},"OrderedColumns":[{"Name":"timestamp","Type":"System.DateTime","CslType":"datetime"},{"Name":"message","Type":"System.String","CslType":"string"}]}}}}}

*/

// Kafka

exports.conf = {
  "Databases": {
    "file": {
      "Name": "file",
      "Tables": {
        "sonicwall": {
          "Name": "sonicwall",
          "Type": "file",
          "Handle": "/Users/prasad/Downloads/3m_small.log",
          "OrderedColumns": [
            {
              "Name": "line",
              "Type": "System.String",
              "CslType": "string"
            }
          ]
        },
        "KafkaExport": {
          "Name": "KafkaExport",
          "Type": "kafka",
          "Handle": {
            "kafkaUrl": "localhost:9092",
            "topic": "__TOPIC__"
          },
          "OrderedColumns": []
        }
      }
    }
  }
}

// Solr
/*exports.conf =  { 
    "bolt_an" : { 
        "type" : "solr",  
        "handle" : "bolt_an" , 
        "schema" : { 
            "timestamp" : "datetime", 
            "jobId" : "string"
        } 
    } 
}*/
