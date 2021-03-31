const Generic = require('./kustoEvaluator.js').generic;
const traverse = require('./kustoEvaluator.js').traverse;
const stringToDataType = require('./kustoEvaluator.js').stringToDataType;
var EventEmitter = require('events');
const csv = require('csv-parser');
const fs = require('fs')
const LineByLineReader = require('line-by-line');
const Kafka = require('node-rdkafka');
const solr = require('solr-node');
const http = require('http')
  , JSONStream = require('JSONStream');
const https = require('https');
const querystring = require('querystring');
const equal = require('deep-equal');
const AWS = require('aws-sdk');
const zlib = require('zlib');
var splunkjs = require('splunk-sdk');
const { debug } = require('console');
const mongodb = require('mongodb').MongoClient;
const avro = require('avsc');
const TopicPartition = require('node-rdkafka/lib/topic-partition.js');
const crypto = require('crypto');


function pivotTable(inputTable) {
    var outputTable = {};
    var outputHeaders;

    outputHeaders =  { "__header__" : [ ], "__key__" : [ ]} ;
    
    // First row is the header row
    var stringCols = [];
    var datetimeCols = [];
    for (var col of inputTable[0]["__header__"]) {
        if (col['type'] == "string")
            stringCols.push(col['name']);
        else if (col['type'] == "datetime") {
            datetimeCols.push(col['name']);
            outputHeaders["__key__"].push(col['name']);
            outputHeaders["__header__"].push( { "name" : col['name'], "type": "datetime" });
        }
    }

    for (var i=1; i < inputTable.length; ++i) {
        var headerCol = "";
        for (var j=0; j < stringCols.length; ++j) {
            if (j > 0)
                headerCol += "_";
            headerCol += inputTable[i][stringCols[j]];
        }
        var datetimeKey = "";
        for (var j=0; j < datetimeCols.length; ++j) {
            if (j > 0)
                headerCol += "_";
            datetimeKey += inputTable[i][datetimeCols[j]];
        }
        if (outputTable[datetimeKey] == null)
            outputTable[datetimeKey] = {};
        var outputRow = outputTable[datetimeKey];

        //for (var key of Object.keys(inputTable[i])) {
        for (var j=0; j < inputTable[0]["__header__"].length; ++j) {
            var key = inputTable[0]["__header__"][j]['name'];
            var type = inputTable[0]["__header__"][j]['type'];

            if (key.startsWith("__"))
                continue;

            if (stringCols.includes(key)) {
                continue;
            }
            else if (datetimeCols.includes(key)) {
                outputRow[key] = inputTable[i][key];
            } 
            else {
                outputRow[headerCol + "_" + key] = inputTable[i][key];
                var col = { "name" : headerCol + "_" + key, "type" : type};
                if (!outputHeaders["__header__"].find( (el) => { return equal(el, col); })) {
                    outputHeaders["__header__"].push(col);
                }
            }
        }
    }

    var returnTable = Object.values(outputTable);
    returnTable.unshift(outputHeaders);
    return returnTable;
}

class KafkaHandler extends EventEmitter {
    evaluatorTree;
    table;
    kafkaUrl;
    topic;
    startTime;
    endTime;
    columns;
    consumer;
    pivot;
    realTime;
    producer = null;
    producerReady = false;

    from_offset = 0;
    to_offset = 0;
    visited = -1;
    numPartitions = 1;
    fromTimestamp; //1199133000000;
    toTimestamp; //1199140200000;
    currentCount = 0;
    batchSize = 100; // This is Kafka polling batch size
    processingBatchSize = 10;
    inProgress = true;
    lastUpdate = Date.now();
    wsData;
    assignmentList = [];
    partitionsCompleted = new Set();
    avroType;
    committedOffsetMap = null;
    tableAlias = null;

    constructor(evaluatorTree, table, kafkaUrlTopic, columns, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.table = table;
        this.tableAlias = table;
        this.columns = columns;
        //{this.kafkaUrl, this.topic, this.groupId, this.numPartitions} = kafkaUrlTopic;
        Object.assign(this, kafkaUrlTopic);
        if (this.groupId == '') {
            this.groupId = Math.random().toString();
            //console.log("Assign group id " + this.groupId);
        }
        if (this.avroSpec != null) {
            this.avroType = avro.parse(this.avroSpec);
        }
        this.startTime = typeof startTime === 'number' ? startTime : Date.parse(startTime);
        this.endTime = typeof endTime === 'number' ? endTime : Date.parse(endTime);
        this.pivot = pivot;
        this.realTime = realTime;
        
    }

    setTableAlias(alias) {
        this.tableAlias = alias;
    }

    cancel() {
        this.inProgress = false;
    }
    process() {
        this.consumer = new Kafka.KafkaConsumer({
            //'group.id': Math.random().toString(),
            'group.id' : this.groupId,
            'metadata.broker.list': this.kafkaUrl, //192.168.33.15:9090
            'enable.auto.commit': true,
            'queued.max.messages.kbytes': 10240,
        }, { 'auto.offset.reset' : 'earliest'});
        
        var timeout = 20000; // 10 seconds
        this.consumer.connect();
        
        this.from_offset = 0;
        this.to_offset = 0;
        this.visited = -1;
        this.fromTimestamp = this.startTime; //1199133000000;
        this.toTimestamp = this.endTime; //1199140200000;
        this.currentCount = 0;
        this.batchSize = 100;
        this.inProgress = true;
        this.lastUpdate = Date.now();
    
        //console.log('-------- StartTime: ' + this.fromTimestamp)
        //console.log('--------- EndTime: ' + this.toTimestamp)
        
        this.wsData =  { "__type__": "table"};
        this.wsData[this.tableAlias] = [];
        
        this.consumer
            .on('ready', async () => {
            //console.log("Topic is " + this.topic);
            var tpoList = [];
            for (var i=0; i < this.numPartitions; ++i) {
                tpoList.push({topic: this.topic, partition: i, offset: this.fromTimestamp });
            }
                /*
            if (!this.realTime) {
                this.consumer.offsetsForTimes(
                    this.tpoList,
                    timeout,
                    this.countOffset.bind(this));
                
                while(this.visited!=1) {
                        await new Promise(r => setTimeout(r, 200));
                }
            }
                */
            var tpObjectList = [];
            for (var i=0; i < this.numPartitions; ++i) {
                tpObjectList.push(TopicPartition.create( { topic: this.topic, partition: i, offset: 0}));
            }

            // Wait for topic to be available
            while(this.visited!=0) {
                this.waitForTopicCreation();
                await new Promise(r => setTimeout(r, 1000));
            }

            this.consumer.committed(tpObjectList, timeout, (err, offsetMap) => {
                this.committedOffsetMap = offsetMap;
                this.visited = 1;
            });

            while(this.visited!=1) {
                await new Promise(r => setTimeout(r, 200));
            }
            
            this.consumer.offsetsForTimes(
                tpoList,
                timeout,
                this.offsetCallback.bind(this));
        
            while(this.visited!=2) {
                await new Promise(r => setTimeout(r, 200));
            }
        
            //console.log("assigning offset : " + this.from_offset)
            await this.consumer.assign(this.assignmentList);
        
            //have to add sleep here, otherwise consume doesn't work
            //await new Promise(r => setTimeout(r, 5000));
        
            this.consumer.consume(this.batchSize, this.callbackConsume.bind(this));
            while(this.inProgress) {
                //consumer.consume(this.batchSize, this.callbackConsume.bind(this));
                await new Promise(r => setTimeout(r, 1000));
                if (!this.realTime && (Date.now() - this.lastUpdate) > 10*1000)
                    this.inProgress = false;
            }

            var data =  { "__type__": "table"};
            data[this.tableAlias] = [];

            var result = this.evaluatorTree.flush(data, []);

            if (result && result.length > 1)
                this.emit("data", this.pivot ? pivotTable(result): result);
        
            this.emit('end');
            //console.log("Disconnecting ")
            this.consumer.unassign();
            this.consumer.disconnect();
            this.consumer = null;
        })        
    }

    waitForTopicCreation() {
        this.consumer.getMetadata(null,(err, metadata) => {
            // Now you have the metadata
            for (var i=0; i < metadata.topics.length; ++i) {
                if (metadata.topics[i]['name'] == this.topic) {
                    console.log("Topic found proceeding ...");
                    this.visited = 0;
                    return;
                }
            }
         });
    }
    /*
    countOffset(err, offsetMap){
        console.log("to offset is " + offsetMap[0]["offset"]);
        this.to_offset = +offsetMap[0]["offset"]
        this.visited = 1;
    }
    */
    
    offsetCallback(err, offsetMap){
        for (var i=0; i < this.numPartitions; ++i) {
            //console.log("from offset is : " + offsetMap[i]["offset"]);
            var offset;
            if (this.committedOffsetMap[i]["offset"] != null) {
                //console.log("resume offset is : " + this.committedOffsetMap[i]["offset"]);
                offset = this.committedOffsetMap[i]["offset"];
            }
            else {
                //console.log("from offset is : " + offsetMap[i]["offset"]);
                offset = offsetMap[i]["offset"];
            }
            this.assignmentList.push({
                topic: this.topic,
                partition: i,
                offset: offset,
            });
            //this.from_offset = +offsetMap[0]["offset"];
        }
        this.visited = 2;
    }
    
    callbackConsume(err, messages) {
        console.log("In consumerCallback " + messages.length);
        console.log("In consumerCallback Error " + err);

        this.currentCount += messages.length;
        if (messages.length > 0)
            this.lastUpdate = Date.now();
    
        messages.forEach(async (message) => {
            if (this.realTime || message.timestamp <= this.toTimestamp) {
                var messageObj = null;
                if (this.avroType == null) {
                    messageObj = JSON.parse(message.value.toString());

                    // For internal Kafka source in which headers will come as message
                    if (messageObj["__header__"]) {
                        this.columns = [];
                        for (var i=0; i < messageObj["__header__"].length; ++i) {
                            this.columns.push( { "Name" : messageObj["__header__"][i]["name"], "CslType" : messageObj["__header__"][i]["type"]});
                        }
                        return;
                    }

                    for (var i=0; i < this.columns.length; ++i) {
                        if (this.columns[i].CslType == 'datetime')
                            messageObj[this.columns[i].Name] = Date.parse(messageObj[this.columns[i].Name]);
                    }
                    this.wsData[this.tableAlias].push(messageObj);
                }
                else {
                    messageObj = this.avroType.fromBuffer(message.value);
                    if (messageObj["contents"] != null) {
                        // TBD:: Special case for handling array of records published by widget framework
                        for (var i=0; i < messageObj["contents"].length; ++i) {
                            for (var j=0; j < this.columns.length; ++j) {
                                if (this.columns[j].CslType == 'datetime')
                                    messageObj["contents"][i][this.columns[j].Name] = Date.parse(messageObj["contents"][i][this.columns[j].Name]);
                            }
                            this.wsData[this.tableAlias].push(messageObj["contents"][i]);
                        }
                    }
                }

                //console.log(messageObj);
            }
            else if (this.partitionsCompleted.size == this.numPartitions) {
                this.inProgress = false;
            }
            else {
                // Mark this message's partition completed and also remove this partition from assignment list
                // So that we don't get any further messages from this partition
                if (!this.partitionsCompleted.has(message.partition)) {
                    //console.log("Done with partition: " + message.partition);
                    this.partitionsCompleted.add(message.partition);
                    /*
                    for (var i=0; i < this.assignmentList.length; ++i) {
                        if (this.assignmentList[i].partition == message.partition) {
                            this.assignmentList.splice(i, 1);
                            await this.consumer.assign(this.assignmentList);    
                        }
                    }
                    */
                }
            }
        })
    
        if ((this.wsData[this.tableAlias].length >= this.processingBatchSize) || (!this.inProgress)) {
            const result = this.evaluatorTree.evaluate(this.wsData, {});
            if (result && result.length > 1)
               this.emit("data", this.pivot ? pivotTable(result) : result);
            /*
            resultMap[panelId]['conn'].sendUTF(JSON.stringify(
                {active: 0, data: result}));
                */
            this.wsData[this.tableAlias] = [];
        }

        // Schedule next batch to process
        if (this.inProgress)
            this.consumer.consume(this.batchSize, this.callbackConsume.bind(this));

    }

    update(inputData) {
        //console.log(inputData);
        if (this.producer == null) {
            this.producer = new Kafka.Producer({
                'metadata.broker.list': this.kafkaUrl,
                'dr_cb': true
              });
        }
        if (this.producerReady == false) {
            // Connect to the broker manually
            this.producer.connect();

            // Wait for the ready event before proceeding
            this.producer.on('ready', () => {
                this.producerReady = true;
                this.sendMessage(inputData);
            });

            // Any errors we encounter, including connection errors
            this.producer.on('event.error', function(err) {
            console.error('Error from producer');
            console.error(err);
            })

            // We must either call .poll() manually after sending messages
            // or set the producer to poll on an interval (.setPollInterval).
            // Without this, we do not get delivery events and the queue
            // will eventually fill up.
            this.producer.setPollInterval(100);
        }
        else {
            this.sendMessage(inputData);
        }
    }

    sendMessage(inputData) {
        // Find the timestamp column from header
        var timestampColumn = null;
        for (var i=0; i < inputData[0]["__header__"].length; ++i) {
            if (inputData[0]["__header__"][i]["type"] == "datetime") {
                timestampColumn = inputData[0]["__header__"][i]["name"];
                break;
            }
        }
        for (var i=0; i < inputData.length; ++i) {
            try {
                // Note: Send timestamp of the first record as timestamp of the header record
                var timestampValue = timestampColumn != null ? inputData[i == 0 ? i + 1 : i][timestampColumn].getTime() : null;
                this.producer.produce(
                    // Topic to send the message to
                    this.topic,
                    // optionally we can manually specify a partition for the message
                    // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
                    null,
                    // Message to send. Must be a buffer
                    Buffer.from(JSON.stringify(inputData[i])),
                    // for keyed messages, we also specify the key - note that this field is optional
                    inputData[i]["__key__"], // TBD: Need to check this
                    // you can send a timestamp here. If your broker version supports it,
                    // it will get added. Otherwise, we default to 0
                    timestampValue,
                    // you can send an opaque token here, which gets passed along
                    // to your delivery reports
                    );
            } catch (err) {
                console.error('A problem occurred when sending our message');
                console.error(err);
            }
        }
    }

}

class FileHandler extends EventEmitter {
    evaluatorTree;
    table;
    filePath;
    columns;
    readStream;
    pivot;
    realTime;
    batchSize = 10;

    constructor(evaluatorTree, table, filePath, columns, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.table = table;
        this.filePath = filePath;
        this.columns = columns;
        this.pivot = pivot;
        this.realTime = realTime;
    }

    cancel() {
        if (this.readStream) {
            this.readStream.destroy();
        }
    }

    process() {
        var data =  { "__type__": "table"};
        data[this.table] = [];
    
        if (this.filePath.endsWith(".csv")) {
        this.readStream = fs.createReadStream(this.filePath);
        this.readStream.pipe(csv())
          .on('data', (row) => {  
    
            //debug_log("INPUT: " + JSON.stringify(row));
            // 2007-01-01T00:00:00Z
            //debug_log(Date.parse(row['StartTime']));
            for (var i=0; i < this.columns.length; ++i) {
                if (this.columns[i].CslType == 'datetime')
                    row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
            }
            data[this.table].push(row);
          
            if (data[this.table].length == this.batchSize) {
              var result = this.evaluatorTree.evaluate(data, {});
              if (result && result.length > 1)
                this.emit("data", this.pivot == true ? pivotTable(result): result);
  
              //console.log("OUTPUT: " + JSON.stringify(result));
              data[this.table] = [];
            }
          
          ////debug_log("OUTPUT: " + JSON.stringify(evaluatorTree.evaluate({"__type__" : "table", "StormEvents" : row}, {})));
        })
        .on('end', () => {
          //console.log(new Date());
          var result = this.evaluatorTree.evaluate(data, {});
          if (result && result.length > 1)
            this.emit("data", this.pivot ? pivotTable(result): result);
          this.emit('end');
          //console.log("OUTPUT: " + JSON.stringify(output, null, 1));
          //console.log(new Date());
    
    
          //console.log(JSON.stringify(output));
        });
      }
      else if (this.filePath.endsWith(".json")) {
        var lr = new LineByLineReader(this.filePath);
    
        lr.on('error', function (err) {
            // 'err' contains error object
        });
        
        lr.on('line', (line) => {
            // pause emitting of lines...
          var row;
          try {
              row = JSON.parse(line);
          }
          catch (err) {
              return;
          }
          for (var i=0; i < this.columns.length; ++i) {
              if (this.columns[i].CslType == 'datetime')
                  row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
          }
          data[this.table].push(row);
        
          if (data[this.table].length == this.batchSize) {
            lr.pause();
            var begin = Date.now()
            var result = this.evaluatorTree.evaluate(data, {});
            if (!result) {
                lr.resume();
                lr.close();
                return;
            }
            if (result && result.length > 1)
                this.emit("data", this.pivot ? pivotTable(result): result);
            
            //console.log("Evaluator took " + (Date.now()-begin));
            //console.log("OUTPUT: " + result);
            data[this.table] = [];
            lr.resume();
          }
      /*
            // ...do your asynchronous line processing..
            setTimeout(function () {
        
                // ...and continue emitting lines.
                lr.resume();
            }, 100);
        */
        });
        
        lr.on('end',  () => {
            // All lines are read, file is closed now.
            //console.log(new Date());
            var result = this.evaluatorTree.evaluate(data, {});
            if (result && result.length > 1)
                this.emit("data", this.pivot == true ? pivotTable(result): result);
            this.emit("end");

            //console.log("OUTPUT: " + JSON.stringify(result, null, 1));
            //console.log(new Date());
      
            //console.log("End");
        })    
        } 
        else {
            var lr = new LineByLineReader(this.filePath);
        
            lr.on('error', function (err) {
                // 'err' contains error object
            });
            
            lr.on('line', (line) => {
                // pause emitting of lines...            
              data[this.table].push({ "line" : line} );
            
              if (data[this.table].length == 10000) {
                lr.pause();
                var begin = Date.now()
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot ? pivotTable(result): result);
                //this.evaluatorTree.printStats();
                //console.log("Evaluator took " + (Date.now()-begin));
                //console.log("OUTPUT: " + result);
                data[this.table] = [];
                lr.resume();
              }
          /*
                // ...do your asynchronous line processing..
                setTimeout(function () {
            
                    // ...and continue emitting lines.
                    lr.resume();
                }, 100);
            */
            });
            
            lr.on('end',  () => {
                // All lines are read, file is closed now.
                //console.log(new Date());
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot == true ? pivotTable(result): result);
                this.emit("end");
    
                //console.log("OUTPUT: " + JSON.stringify(result, null, 1));
                //console.log(new Date());
          
                //console.log("End");
            })    
            }        
    }
}

class SplunkHandler extends EventEmitter {
    evaluatorTree;
    table;
    filePath;
    columns;
    pivot;
    realTime;
    batchSize = 1000;
    startTime;
    endTime;
    queryText;
    timestampColumn;
    request;

    constructor(evaluatorTree, table, splunkParams, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.table = table;
        this.columns = columns;
        this.pivot = pivot;
        this.realTime = realTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.queryText = queryText;
        this.timestampColumn = timestampColumn;

        // This will set username, password, scheme and host parameters
        Object.assign(this, splunkParams);

    }

    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }

    process() {
        var params =  {
            search: this.queryText,
            earliest_time : this.startTime,
            latest_time : this.endTime,
            output_mode : 'json'
        };

        var auth = 'Basic ' + Buffer.from(this.username + ':' + this.password).toString('base64');

        var postParams = querystring.stringify(params);
        //console.log(querystring.stringify(params));
        const options = {
            hostname: this.host,
            port: 8089,
            path: "/servicesNS/admin/search/search/jobs/export",
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Content-Length': postParams.length,
                'Authorization': auth
            },
            rejectUnauthorized: false // To accept self signed certificates
        };

        var data =  { "__type__": "table"};
        data[this.table] = [];

        this.request = https.request(options, (res) => {
        res.pipe(JSONStream.parse('*')).on("data", (row) => {
            // Export return row number, true/false flag and then the row
            if (typeof row != 'object')
                return;
                
            for (var i=0; i < this.columns.length; ++i) {
                if (this.columns[i].CslType == 'datetime')
                    row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
            }
            data[this.table].push(row);
          
            if (data[this.table].length == this.batchSize) {
              var result = this.evaluatorTree.evaluate(data, {});
              if (!result) {
                  //this.emit("end");
                  this.cancel();
                  return;
              }
              if (result && result.length > 1)
                  this.emit("data", this.pivot ? pivotTable(result): result);
              
              //console.log("Evaluator took " + (Date.now()-begin));
              //console.log("OUTPUT: " + result);
              data[this.table] = [];
            }
          })
          .on("end", () => {
            var begin = Date.now()
            var result = this.evaluatorTree.evaluate(data, {});
            if (result && result.length > 1)
                this.emit("data", this.pivot ? pivotTable(result): result);
            
            //console.log("End Evaluator took " + (Date.now()-begin));
            //console.log("OUTPUT: " + result);
            data[this.table] = [];        
            this.emit("end");            
          }).on("error", () => {
            console.log("Error in query");
            this.emit("end");
        })
        });
        this.request.write(postParams);
        this.request.end();
    }

    process_blocking() {
        // Create a Service instance and log in 
        var service = new splunkjs.Service({
            username: this.username,
            password: this.password,
            scheme: this.scheme,
            host: this.host,
            port:"8089"
        });

        // Print installed apps to the console to verify login
        service.login((err, success) => {
            if (err) {
                throw err;
            }

            var data =  { "__type__": "table"};
            data[this.table] = [];    

            // Set the search parameters
            var searchParams = {
                exec_mode: "blocking",
                earliest_time: this.startTime,
                latest_time: this.endTime
            };

            // Run a normal search that immediately returns the job's SID
            service.search(
                this.queryText,
                searchParams,
                (err, job) => {
                    if (err) {
                        throw err;
                    }

                    // Display the job's search ID
                    console.log("Job SID: ", job.sid);

                    // Poll the status of the search job
                    job.fetch((err) => {
                        if (err) {
                            throw err;
                        }    
                        console.log("Done!");

                        // Print out the statics
                        console.log("Job statistics:");
                        console.log("  Event count:  " + job.properties().eventCount); 
                        console.log("  Result count: " + job.properties().resultCount);
                        console.log("  Disk usage:   " + job.properties().diskUsage + " bytes");
                        console.log("  Priority:     " + job.properties().priority);

                        // Page through results by looping through sets of 10 at a time
                        var resultCount = job.properties().resultCount; // Number of results this job returned
                        var myOffset = 0;         // Start at result 0
                        var myCount = this.batchSize;         // Get sets of 10 results at a time

                        // Run an asynchronous while loop using the Async.whilst helper function to
                        // loop through each set of results 
                        splunkjs.Async.whilst(

                            // Condition--loop while there are still results to display
                            () => {
                                return (myOffset < resultCount);
                            },

                            // Body--display each set of results
                            (done) => {
                                console.log("Current offset " + myOffset);
                                // Get the results and print them
                                job.results({count: myCount,offset:myOffset}, (err, results, job) => {
                                    var fields = results.fields;
                                    var fieldLookup = {};
                                    for (var i=0; i < fields.length; ++i) {
                                        fieldLookup[fields[i]] = i;
                                    }
                                    var rows = results.rows;
                                    var newRow = {};
                                    for(var i = 0; i < rows.length; i++) {
                                        for (var j=0; j < this.columns.length; ++j) {
                                            if (this.columns[j].CslType == 'datetime')
                                                newRow[this.columns[j].Name] = Date.parse(rows[i][fieldLookup[this.columns[j].Name]]);
                                            else
                                                newRow[this.columns[j].Name] = rows[i][fieldLookup[this.columns[j].Name]];
                                        }
                                        data[this.table].push(newRow);
                            
                              
                                        /*
                                        var values = rows[i];
                                        console.log("Row " + i + ": ");
                                        for(var j = 0; j < values.length; j++) {
                                        var field = fields[j];
                                        var value = values[j];
                                        console.log("  " + field + ": " + value);
                                        }*/
                                    }
                                    var result = this.evaluatorTree.evaluate(data, {});
                                    if (!result) {
                                        myOffset = Number.MAX_SAFE_INTEGER;
                                        done();
                                        return;
                                    }
                                    if (result && result.length > 1)
                                        this.emit("data", this.pivot ? pivotTable(result): result);

                                    // Increase the offset to get the next set of results
                                    // once we are done processing the current set.
                                    myOffset = myOffset + myCount;
                                    done();
                                })
                            },
                            // Done
                            (err) => { 
                                console.log("Query finished!")
                                this.emit("end");
                                if (err) console.log("Error: " + err);
                            }
                        );
                    });
                });
        });
            /*
        var data =  { "__type__": "table"};
        data[this.table] = [];
    
        this.readStream = fs.createReadStream(this.filePath);
        var lr = new LineByLineReader(this.filePath);
    
        lr.on('error', function (err) {
            // 'err' contains error object
        });
        
        lr.on('line', (line) => {
            // pause emitting of lines...
          var row;
          try {
              row = JSON.parse(line);
          }
          catch (err) {
              return;
          }
          for (var i=0; i < this.columns.length; ++i) {
              if (this.columns[i].CslType == 'datetime')
                  row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
          }
          data[this.table].push(row);
        
          if (data[this.table].length == this.batchSize) {
            lr.pause();
            var begin = Date.now()
            var result = this.evaluatorTree.evaluate(data, {});
            if (!result) {
                lr.resume();
                lr.close();
                return;
            }
            if (result && result.length > 1)
                this.emit("data", this.pivot ? pivotTable(result): result);
            
            //console.log("Evaluator took " + (Date.now()-begin));
            //console.log("OUTPUT: " + result);
            data[this.table] = [];
            lr.resume();
          }
      
        });
        
        lr.on('end',  () => {
            // All lines are read, file is closed now.
            //console.log(new Date());
            var result = this.evaluatorTree.evaluate(data, {});
            if (result && result.length > 1)
                this.emit("data", this.pivot == true ? pivotTable(result): result);
            this.emit("end");

            //console.log("OUTPUT: " + JSON.stringify(result, null, 1));
            //console.log(new Date());
      
            //console.log("End");
        })    
        */
    }
}

class DedupSolrHandler extends EventEmitter{
    batchSize;
    host;
    port;
    collection;
    queryText;
    client;
    evaluatorTree;
    columns;
    table;
    timestampColumn;
    fl;
    sort;
    currentEndTime;
    timeIncrement;
    inProgress;

    constructor(evaluatorTree, table, collection, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.batchSize = 10000;
        // time Increment unit is milliseconds
        this.timeIncrement = 60*60*1000;
        this.currentCursor = 0;
        this.numFound = Number.MAX_SAFE_INTEGER;
        [this.host, this.port, this.collection, this.collectionWindow] = collection.split('#');
        this.queryText = queryText != null ? queryText : "*:*";
        if (startTime && endTime) {
            this.currentStartTime = Date.parse(startTime);
            this.currentEndTime = this.currentStartTime + this.timeIncrement;
            this.endTime = Date.parse(endTime);
        }
        if (timestampColumn)
            this.sort = "sort=timestamp+desc";

        this.fl = "";
        for (var column of Object.keys(columns)) {
            if (this.fl != "")
                this.fl += ","
            this.fl += column;
        }

        //this.queryText += " AND " + timestampColumn + ":[" + startTime + " TO " + endTime + "]";
        this.table = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
    
        // Create client
       this.client = new solr({
            host: this.host,
            port: this.port,
            core: this.collection,
            
            protocol: 'http'
        });

       this.inProgress = true;
    }

    cancel() {
        //console.log("Destroying request");
	this.inProgress = false;
    }

    processBatch(err, result) {
	if (!this.inProgress)
	    return;

        if (err) {
            console.log(err);
            return;
        }
        //console.log(JSON.stringify(result));
        var data =  { "__type__": "table"};
        // Splice in the header column
        if (result.response == null) {
            this.numFound = -1;
            this.currentStartTime = this.currentEndTime;
            this.process();
            return;
        }
        this.numFound = result.response.numFound;
        this.currentCursor += result.response.docs.length;

        for (var i=0; i < result.response.docs.length; ++i) {
            if (result.response.docs[i][this.timestampColumn] != null) {
                result.response.docs[i][this.timestampColumn] = Date.parse(result.response.docs[i][this.timestampColumn]);
            }
        }
        //console.log(JSON.stringify(result));

        //result.response.docs.unshift(this.columns);
        data[this.table] = result.response.docs;
        var begin = Date.now();
        var output = this.evaluatorTree.evaluate(data, {});
        var end = Date.now();
        console.log("Time in evaluation : " + (end-begin));
        if (output != null) {
            this.emit("data", output);
            //console.log("OUTPUT: " + JSON.stringify(output));
        }
        else {
            this.emit("end");
            return;
        }
        this.process();
    }
        
    process() {

        if (this.currentCursor >= this.numFound) {
            if (this.currentStartTime >= this.endTime) {
                this.emit("end");
                return;
            }
            else {
                this.currentStartTime = this.currentEndTime;
                this.currentEndTime = this.currentStartTime + this.timeIncrement;
                if (this.currentEndTime > this.endTime)
                    this.currentEndTime = this.endTime;
                this.currentCursor = 0;
            }
        }

        console.log("Running with " + this.queryText + " " + this.currentCursor + " " + this.batchSize + " " + this.numFound + 
	" " + new Date(this.currentStartTime).toISOString() + " " + new Date(this.currentEndTime).toISOString());
        var query = this.client.query()
            .q(this.queryText)
            .addParams([
                {field: 'collectionWindow', value: this.collectionWindow},
                {field: 'startTime', value: new Date(this.currentStartTime).toISOString()},
                {field: 'endTime', value: new Date(this.currentEndTime).toISOString()},
                {field: 'getRawMessages', value: true}
            ]).start(this.currentCursor).rows(this.batchSize);

        // Bind below is important that changes the context to "this" object
        this.client.search(query, this.processBatch.bind(this)); 

    }
}

class ShardedSolrHandler extends EventEmitter{
    batchSize;
    host;
    port;
    collection;
    queryText;
    client;
    evaluatorTree;
    columns;
    table;
    timestampColumn;
    fl;
    sort;
    currentEndTime;
    timeIncrement;
    inProgress;
    createdCollections = [];
    pendingCreationCollections = [];
    request = null;
    pivot = false;
    unshardedTimeIncrement = 24*60*60*1000;

    constructor(evaluatorTree, table, collection, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.batchSize = 10000;
        this.currentCursor = 0;
        this.numFound = Number.MAX_SAFE_INTEGER;
        [this.host, this.port, this.collection, this.collectionWindow] = collection.split('#');
        // time Increment unit is milliseconds. Make it same as the collection window
        this.timeIncrement = this.collectionWindow*60*60*1000;
        this.queryText = queryText != null ? queryText : "*:*";
        this.queryText = this.queryText.replace(/"/g, "\\\"");
        //this.queryText = "q=" + encodeURIComponent(queryText);

        if (startTime && endTime) {
            this.currentStartTime = Date.parse(startTime);
            this.currentEndTime = this.currentStartTime;
            this.endTime = Date.parse(endTime);
        }
        if (timestampColumn)
            //this.queryText += "&sort=timestamp+asc";
            this.sort = timestampColumn + " asc";

        this.fl = "";
        for (var i=0; i < columns.length; ++i) {
            if (this.fl != "")
                this.fl += ","
            this.fl += columns[i].Name;
        }
        //this.queryText += "&fl=" + this.fl;

        this.table = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
    
        // Create client
       this.client = new solr({
            host: this.host,
            port: this.port,
            core: this.collection,
            
            protocol: 'http'
        });

       this.inProgress = true;
       this.pivot = pivot;
    }

    getCollectionWindow(timestamp) {
        if (this.timeIncrement != 0)
            return this.collection + "_" + new Date(Math.floor(timestamp/this.timeIncrement)*this.timeIncrement).toISOString().substring(0, 13);
        else
            return this.collection;
    }

    getSolrType(kustoType) {
        switch(kustoType) {
            case "string":
                return "string";
            case "datetime":
                return "pdate";
            case "long":
                return "plong";
            case "int":
                return "pint";  
            case "real":
            case "decimal":
                    return "pdouble"; 
            default:
                return "string";
        }
    }

    getSchema() {
        var schema = [];
        for (var i=0; i < this.columns.length; ++i) {
            var field = { "multiValued" : false, "docValues" : true, "indexed" : false, "stored" : true};
            field["name"] = this.columns[i].Name;
            field["type"] = this.getSolrType(this.columns[i].CslType);
            schema.push(field);
            // In case of string, create a separate indexed field of type "text_general"
            if (this.columns[i].CslType == "string") {
                field = { "multiValued" : false, "docValues" : false, "indexed" : true, "stored" : false};
                field["name"] = this.columns[i].Name + "_str";
                field["type"] = "text_general";
                schema.push(field);
            }
        }
        return { "add-field" : schema};
    }
    
    createConfig(collection, schema, data) {
        console.log("Creating config");
        this.httpGet("http://" + this.host + ":" + this.port + "/solr/admin/configs?action=CREATE&name=" + collection + "&baseConfigSet=alertlens&maxShardsPerNode=2&numShards=2&replicationFactor=1", 
            this.createCollection.bind(this, collection, schema, data));
    }
    
    createCollection(collection, schema, data) {
        console.log("Creating collection");
        this.httpGet("http://" + this.host + ":" + this.port + "/solr/admin/collections?action=CREATE&name=" + collection + "&collection.configName=" + collection + "&maxShardsPerNode=2&numShards=2&replicationFactor=1", 
            this.createSchema.bind(this, collection, schema, data));
    }
    
    createSchema(collection, schema, data) {
        console.log("Creating schema");
        this.httpPost(this.host, this.port, "/solr/" + collection + "/schema", JSON.stringify(schema), 
            this.updateAutoCreateFields.bind(this, collection, data));
    }
    
    updateAutoCreateFields(collection, data) {
        console.log("Updating autoCreateFields");
        this.httpPost(this.host, this.port, "/solr/" + collection + "/config", '{"set-user-property": {"update.autoCreateFields":"true"}}', 
            this.updateOpenSearcher.bind(this, collection, data));
    }
    
    updateOpenSearcher(collection, data) {
        console.log("Updating updateOpenSearcher");
	// COllection has been created. Remove it from pending list
        const index = this.pendingCreationCollections.indexOf(collection);
        if (index > -1) {
          this.pendingCreationCollections.splice(index, 1);
        }
        this.httpPost(this.host, this.port, "/solr/" + collection + "/config", '{ "set-property" : { "updateHandler.autoCommit.openSearcher":true }}', 
            this.updateRecords.bind(this, collection, data));
    }
    
    httpGet(url, callback) {
            http.get(url, res => {
            console.log(res.statusCode);
            res.setEncoding('utf8');
              let rawData = '';
              res.on('data', (chunk) => { rawData += chunk; });
              res.on('end', () => {
                try {
                  const parsedData = JSON.parse(rawData);
                  console.log(parsedData);
                  if (callback != null) 
                    callback();
                } catch (e) {
                  console.error(e.message);
                }
              });
        });
    }
    
    httpPost(host, port, path, data, callback) {
        const options = {
            hostname: host,
            port: port,
            //path: "/solr/" + this.collection + "/update?commit=true",
            path: path,
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': data.length
            }
          };
    
        console.log(JSON.stringify(options));
          
        const req = http.request(options, async (res) => {
            console.log(`statusCode: ${res.statusCode}`);
              
            res.on('data', (d) => {
                      process.stdout.write(d);
                });
            res.on('end', (res) => {
                if (callback != null) 
                    callback();
            });
        });
        req.write(data);
        req.end();
    }

    update(inputData) {
        // Remove the header
        inputData.splice(0, 1);
        var collectionWindows = {}
        // Segregate data by collection window
        for (var i=0; i < inputData.length; ++i) {
            var window = this.getCollectionWindow(inputData[i][this.timestampColumn]);
            // Convert datetime value into a datetime string for Solr
	        if (inputData[i][this.timestampColumn] != null) {
                inputData[i][this.timestampColumn] = new Date(inputData[i][this.timestampColumn]).toISOString();
            }
            // For each string type create a separate field for indexing
            for (var j=0; j < this.columns.length; ++j) {
                if (this.columns[j].CslType == "string") {
                    if (inputData[i][this.columns[j].Name] != null) {
                        inputData[i][this.columns[j].Name + "_str"] = inputData[i][this.columns[j].Name];
                    }
                }
            }
            if (collectionWindows[window] == null) {
                collectionWindows[window] = [];
            }
            collectionWindows[window].push(inputData[i]);
        }

        for (var window of Object.keys(collectionWindows)) {
            if (!this.createdCollections.includes(window)) {
                this.createConfig(window, this.getSchema(), collectionWindows[window]);
                this.createdCollections.push(window);
                this.pendingCreationCollections.push(window);
            }
            this.updateRecords(window, collectionWindows[window]);
        }

    }

    async updateRecords(collection, inputData) {
        const data = JSON.stringify(inputData);
        const options = {
            hostname: this.host,
            port: this.port,
            path: "/solr/" + collection + "/update",
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': data.length
            }
          };

	  if (this.pendingCreationCollections.includes(collection)) {
            await new Promise(r => setTimeout(r, 1000));
            this.updateRecords(collection, inputData);
            return;
	  }
          
          const req = http.request(options, async (res) => {
            //console.log(`statusCode: ${res.statusCode}`);
            if (res.statusCode != 200) {
		// Schedule retry 
                await new Promise(r => setTimeout(r, 1000));
                this.updateRecords(collection, inputData);
	    }
          
            res.on('data', d => {
              //process.stdout.write(d);
            })
          });
          
          req.on('error', error => {
            console.error(error)
          });
          
          req.write(data);
          req.end();
    }

    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }
        
    process() {
        var data =  { "__type__": "table"};
        data[this.table] = [];

        if (this.currentStartTime >= this.endTime) {
            data =  { "__type__": "table"};
            data[this.table] = [];
            var result = this.evaluatorTree.flush(data, []);

            if (result && result.length > 1)
                this.emit("data", this.pivot ? pivotTable(result): result);

            this.emit("end");
            return;
        }
        else {
            this.currentStartTime = this.currentEndTime;
            if (this.timeIncrement != 0)
                this.currentEndTime = Math.floor((this.currentStartTime + this.timeIncrement)/this.timeIncrement)*this.timeIncrement;
            else
                this.currentEndTime = this.currentStartTime + this.unshardedTimeIncrement;

            if (this.currentEndTime > this.endTime)
                this.currentEndTime = this.endTime;
        }

        var currentQueryText = this.queryText + " AND " + this.timestampColumn + ":[" + new Date(this.currentStartTime).toISOString() 
                            + " TO " + new Date(this.currentEndTime).toISOString() + "]";
        /*
        this.url = "http://" + this.host + ":" + this.port + "/solr/" + this.getCollectionWindow(this.currentStartTime) 
                    + "/export/?" + this.queryText 
                    + "&fq=" + encodeURIComponent(this.timestampColumn + ":[" + new Date(this.currentStartTime).toISOString() 
                                                + " TO " + new Date(this.currentEndTime).toISOString() + "]");
        */
       const postParams = encodeURI('expr=search(' + this.getCollectionWindow(this.currentStartTime) + ', q="' + currentQueryText + '", fl="' + this.fl + '", sort="' + this.sort + '", qt="/export")');
       const options = {
            hostname: this.host,
            port: this.port,
            //path: "/solr/" + this.collection + "/update?commit=true",
            path: "/solr/" + this.getCollectionWindow(this.currentStartTime) + "/stream/",
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Content-Length': postParams.length
            }
        };


        console.log("Running with " + this.queryText + " " 
            + new Date(this.currentStartTime).toISOString() + " " + new Date(this.currentEndTime).toISOString());

        this.request = http.request(options, (res) => {
            res.pipe(JSONStream.parse('result-set.docs.*')).on("data", (row) => {
                if (row["EOF"] == true) {
                    return;
                }
                //console.log("Data size is " + data[this.table].length);
                for (var i=0; i < this.columns.length; ++i) {
                    if (this.columns[i].CslType == 'datetime')
                        row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
                }
                data[this.table].push(row);

                if (data[this.table].length == this.batchSize) {
                    var begin = Date.now()
                    console.log(begin);
                    var result = this.evaluatorTree.evaluate(data, {});
                    if (!result) {
                        // Short circuit the processing. Query has indicated no further response required   
                        this.cancel();
                        return;
                    }
                    if (result && result.length > 1)
                        this.emit("data", this.pivot ? pivotTable(result): result);
                    console.log("Data Evaluator took " + (Date.now()-begin));
                    //console.log("OUTPUT: " + output);
                    data[this.table] = [];
                }
            }).on("end", () => {
                var begin = Date.now()
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot ? pivotTable(result): result);
                
                console.log("End Evaluator took " + (Date.now()-begin));
                //console.log("OUTPUT: " + result);
                data[this.table] = [];        
                this.process();
                
        }).on("error", () => {
	     console.log("Error in query");
             this.process();
	})
    });
    this.request.write(postParams);
    this.request.end();
    }

    flush() {
    }
}

class SolrHandler extends EventEmitter{
    batchSize;
    host;
    port;
    collection;
    url;
    queryText;
    client;
    evaluatorTree;
    columns;
    table;
    timestampColumn;
    request = null;
    pivot;
    realTime;

    constructor(evaluatorTree, table, collection, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.batchSize = 10000;
        this.currentCursor = 0;
        this.numFound = Number.MAX_SAFE_INTEGER;
        [this.host, this.port, this.collection] = collection.split('#');
        queryText = queryText != null ? queryText : "*:*";
        this.queryText = "q=" + encodeURIComponent(queryText);
        if (timestampColumn)
            this.queryText += "&sort=timestamp+desc";

        var fl = "";
        for (var i=0; i < columns.length; ++i) {
            if (fl != "")
                fl += ","
            fl += columns[i].Name;
        }
        this.queryText += "&fl=" + fl;
        this.queryText += "&fq=" + encodeURIComponent(timestampColumn + ":[" + startTime + " TO " + endTime + "]");
        this.table = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
        this.pivot = pivot;
        this.realTime = realTime;
    
        this.url = "http://" + this.host + ":" + this.port + "/solr/" + this.collection + "/export/?" + this.queryText;
    }
    
    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }

    update(inputData) {
        // Remove the header
        inputData.splice(0, 1);
        const data = JSON.stringify(inputData);
        const options = {
            hostname: this.host,
            port: this.port,
            path: "/solr/" + this.collection + "/update?commit=true",
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': data.length
            }
          };
          
          const req = http.request(options, res => {
            console.log(`statusCode: ${res.statusCode}`);
          
            res.on('data', d => {
              process.stdout.write(d);
            })
          });
          
          req.on('error', error => {
            console.error(error)
          });
          
          req.write(data);
          req.end();
    }

    process() {
        var data =  { "__type__": "table"};
        data[this.table] = [];

        console.log("Running with " + this.url);
        // Bind below is important that changes the context to "this" object
        this.request = http.get(this.url, (res) => {
            res.pipe(JSONStream.parse('response.docs.*')).on("data", (row) => {
                for (var i=0; i < this.columns.length; ++i) {
                    if (this.columns[i].CslType == 'datetime')
                        row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
                }
                data[this.table].push(row);

                if (data[this.table].length == this.batchSize) {
                    var begin = Date.now()
                    console.log(begin);
                    var result = this.evaluatorTree.evaluate(data, {});
                    if (!result) {
                        // Short circuit the processing. Query has indicated no further response required   
                        this.cancel();
                        return;
                    }
                    if (result && result.length > 1)
                        this.emit("data", this.pivot ? pivotTable(result): result);
                    console.log("Evaluator took " + (Date.now()-begin));
                    //console.log("OUTPUT: " + output);
                    data[this.table] = [];
                }
            }).on("end", () => {
                var begin = Date.now()
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot ? pivotTable(result): result);
                //console.log("Evaluator took " + (Date.now()-begin));
                //console.log("OUTPUT: " + result);
                data[this.table] = [];        
        })});
    }
}

class S3Handler extends EventEmitter{
    bucketPrefix;
    queryText;
    client;
    evaluatorTree;
    columns;
    table;
    timestampColumn;
    request = null;
    pivot;
    realTime;
    s3;
    inProgress = true;
    cacheFolder = "/tmp/s3handler_cache";
    chunkSize = 1*60*60*1000; // In milliseconds
    fileHandleMap = {};
    expiryTime = 5*60*1000; // When to expire a file and upload it
    tableAlias = null;

    constructor(evaluatorTree, table, bucketPrefix, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        this.batchSize = 10000;
        this.currentCursor = 0;
        this.numFound = Number.MAX_SAFE_INTEGER;
        this.bucketPrefix = bucketPrefix;

        this.table = table;
        this.tableAlias = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
        this.pivot = pivot;
        this.realTime = realTime;

        this.startTime = typeof startTime === 'number' ? startTime : Date.parse(startTime);
        this.endTime = typeof endTime === 'number' ? endTime : Date.parse(endTime);

        this.currentObjectPrefix = "";
        this.currentObjectTime = 0;

        this.s3 = new AWS.S3({apiVersion: '2006-03-01', httpOptions: { timeout: 10 * 60 * 1000 }});
        this.objectQueue = [];
        this.inProgress = true;
    
    }

    setTableAlias(alias) {
        this.tableAlias = alias;
    }
    
    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }

    getRandomInt() {
        return Math.floor(Math.random() * Math.floor(1000));
      }
      
    update(inputData) {
        inputData.forEach(async (row) => {
            if (row[this.timestampColumn] == null)
                return;
            var chunkTime = Math.floor(row[this.timestampColumn]/this.chunkSize)*this.chunkSize;
            // objectPath is the folder inside bucket where the file will be stored in S3
            // Folder is the day ...
            var objectPath = this.table + "/" + new Date(chunkTime).toISOString().slice(0, 10) + "/" +
                                    this.table + "_" + new Date(chunkTime).toISOString();
            if (this.fileHandleMap[objectPath] == null) {
                // fileName is name of file under the 'objectPath' folder
                // Same as object path except don't put "/"
                // Add a random integer so that it doesn't conflict with other processes which might upload
                // same time chunk
                var fileName = this.table + "_" + new Date(chunkTime).toISOString();
                // Open the file locally for caching
                var f = fs.openSync(this.cacheFolder + "/" + fileName, "w");
                this.fileHandleMap[objectPath] = {};
                this.fileHandleMap[objectPath]["fileName"] = fileName;
                this.fileHandleMap[objectPath]["fileHandle"] = f;
                this.fileHandleMap[objectPath]["lastUpdateTime"] = Date.now();
            }
            fs.writeSync(this.fileHandleMap[objectPath]["fileHandle"], JSON.stringify(row));
            fs.writeSync(this.fileHandleMap[objectPath]["fileHandle"], "\n");
            this.fileHandleMap[objectPath]["lastUpdateTime"] = Date.now();
        }
        );

        for (var key of Object.keys(this.fileHandleMap)) {
            if ((Date.now() - this.fileHandleMap[key]["lastUpdateTime"]) > this.expiryTime) {
                // Close the file and upload to S3
                fs.closeSync(this.fileHandleMap[key]["fileHandle"]);
                this.uploadToS3(key, this.cacheFolder + "/" + this.fileHandleMap[key]["fileName"]);   
            }
        }
    }

    uploadToS3(objectPath, localFileName) {
        // Gzip the file first
        const gzip = zlib.createGzip();
        const fs = require('fs');
        const inp = fs.createReadStream(localFileName);
        const out = fs.createWriteStream(localFileName + '.gz');

        inp.pipe(gzip).pipe(out).on('finish', () => {

            const gzInp = fs.createReadStream(localFileName + '.gz');
            const bucketParams = {
                Bucket : this.bucketPrefix,
                Key: objectPath + "_" + this.getRandomInt() + ".gz",
                Body: gzInp
            };

            var options = { partSize: 5 * 1024 * 1024, queueSize: 10 };  

            this.s3.upload(bucketParams, options, (err, data) => {  
                console.log('Completed ' + err);  
                delete this.fileHandleMap[objectPath];
                fs.unlink(localFileName, () => { console.log("File removed"); });
                fs.unlink(localFileName + '.gz', () => { console.log("File removed"); });
            });   
        });
    }

    flush() {
        // Upload any remaining files to S3
        for (var key of Object.keys(this.fileHandleMap)) {
            // Close the file and upload to S3
            fs.closeSync(this.fileHandleMap[key]["fileHandle"]);
            this.uploadToS3(key, this.cacheFolder + "/" + this.fileHandleMap[key]["fileName"]);       
        }
    }
    
    process() {
        var data =  { "__type__": "table"};
        data[this.tableAlias] = [];

        if (this.objectQueue.length == 0) {
            // Advance to next object name prefix
            if (this.currentObjectTime != 0) {
                //this.currentObjectTime += this.chunkSize;
                // The archives are organized by day ... so advanced by day
                this.currentObjectTime += (24*60*60*1000);
            }
            else {
                //this.currentObjectTime = Math.floor(this.startTime/(this.chunkSize)) * (this.chunkSize);
                this.currentObjectTime = Math.floor(this.startTime/(24*60*60*1000)) * (24*60*60*1000);
            }

            if (this.currentObjectTime > this.endTime) {
                this.emit("end");
                return ;
            }
            this.currentObjectPrefix = this.table + "/" + new Date(this.currentObjectTime).toISOString().slice(0, 10);
            
            var bucketParams = {
                Bucket : this.bucketPrefix,
                Prefix: this.currentObjectPrefix
            };
            this.s3.listObjectsV2(bucketParams, (err, data) => {
                if (err) {
                    console.log("Error", err);
                }   
                else {
                    for (var i=0; i < data.Contents.length; ++i) {
                        // Key format is archive/2020-11-30/archive_2020-11-30T00:00:00.000Z_362.gz
                        var key = data.Contents[i].Key;
                        var tsStart = this.table.length*2 + 1 + 10 + 1 + 1;
                        var temp = Date.parse(key.slice(tsStart, tsStart+24));
                        if ((temp+this.chunkSize) > this.startTime && temp < this.endTime) {
                            this.objectQueue.push(data.Contents[i].Key);
                        }
                    }
                }
                this.process();
            });
        }
        else {
            if (this.inProgress == false)
                return;

            var params = {
                Bucket: this.bucketPrefix,
                Key: this.objectQueue.shift()
               };
               console.log("Processing file " + params.Key);
            var lr = new LineByLineReader(
                                this.s3.getObject(params).createReadStream()
                                .on('error', function(err) { console.log('err'); })
                                .pipe(zlib.createGunzip())
                                );
            lr.on('error', function (err) {
                // 'err' contains error object
            });
            
            lr.on('line', (line) => {
                // pause emitting of lines...
            if (this.inProgress == false)
                return;

              var row = JSON.parse(line);
              for (var i=0; i < this.columns.length; ++i) {
                  if (this.columns[i].CslType == 'datetime')
                      row[this.columns[i].Name] = Date.parse(row[this.columns[i].Name]);
              }
              if (row[this.timestampColumn] > this.endTime) {
                  lr.close();
                  this.inProgress = false;
      
                  var result = this.evaluatorTree.evaluate(data, {});
                  if (result && result.length > 1)
                      this.emit("data", this.pivot == true ? pivotTable(result): result);
                      
                  data =  { "__type__": "table"};
                  data[this.tableAlias] = [];
      
                  result = this.evaluatorTree.flush(data, []);
                  if (result && result.length > 1)
                    this.emit("data", this.pivot ? pivotTable(result): result);
          
                  //this.emit('end');
        
                  return;
              }
              else if (row[this.timestampColumn] < this.startTime) {
                  return;
              }
              data[this.tableAlias].push(row);
            
              if (data[this.tableAlias].length == 10000) {
                lr.pause();
                var begin = Date.now()
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot ? pivotTable(result): result);
                console.log("Evaluator took " + (Date.now()-begin));
                //console.log("OUTPUT: " + result);
                data[this.tableAlias] = [];
                lr.resume();
              }
          /*
                // ...do your asynchronous line processing..
                setTimeout(function () {
            
                    // ...and continue emitting lines.
                    lr.resume();
                }, 100);
            */
            });
            
            lr.on('end',  () => {
                // All lines are read, file is closed now.
                //console.log(new Date());
                var result = this.evaluatorTree.evaluate(data, {});
                if (result && result.length > 1)
                    this.emit("data", this.pivot == true ? pivotTable(result): result);
                data[this.tableAlias] = [];
    
                this.process();

                    //console.log("OUTPUT: " + JSON.stringify(result, null, 1));
                //console.log(new Date());
          
                //console.log("End");
            });   
        }       
    }
}

class MongoDBHandler extends EventEmitter{
    batchSize;
    host;
    port;
    database;
    collection;
    url;
    queryText;
    client;
    evaluatorTree;
    columns;
    table;
    timestampColumn;
    request = null;
    pivot;
    realTime;

    constructor(evaluatorTree, table, collection, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.evaluatorTree = evaluatorTree;
        [this.host, this.port, this.database, this.collection] = collection.split('#');

        this.table = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
        this.pivot = pivot;
        this.realTime = realTime;
    
        this.url = "mongodb://" + this.host + ":" + this.port;
    }
    
    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }

    update(inputData) {
        // Remove the header
        inputData.splice(0, 1);

        mongodb.connect(this.url, (err, db) => {
            if (err) throw err;
            var dbo = db.db(this.database);
            dbo.collection(this.collection).insertMany(inputData, (err, res) => {
              if (err) throw err;
              console.log("Document inserted " + inputData.length);
              db.close();
            });
          });
    }
}

class UnifiedHandler extends EventEmitter{
    database;
    queryText;
    client;
    evaluatorTree;
    chunkEvaluatorTree = null;
    columns;
    table;
    timestampColumn;
    currentEndTime;
    timeIncrement;
    inProgress;
    request = null;
    pivot = false;
    unshardedTimeIncrement = 24*60*60*1000;
    handler = null;
    resultCacheFolder = "./result_cache"
    resultCache = {};
    resultCacheHeader = null;
    queryTree;
    resultCacheBucket = null;
    s3 = null;

    constructor(queryTree, evaluatorTree, database, table, handle, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
        super();
        this.queryTree = queryTree;
        this.database = database;
        Object.assign(this, handle);

        this.evaluatorTree = evaluatorTree;
        this.batchSize = 10000;
        this.currentCursor = 0;
        this.numFound = Number.MAX_SAFE_INTEGER;

        if (startTime && endTime) {
            this.currentStartTime = Date.parse(startTime);
            this.currentEndTime = this.currentStartTime;
            this.endTime = Date.parse(endTime);
        }

        this.table = table;
        this.columns = columns;
        this.timestampColumn = timestampColumn;
        this.queryText = queryText;
    
        this.inProgress = true;
        this.pivot = pivot;
        this.realTime = realTime;

        if (this.resultCacheBucket) {
            this.s3 = new AWS.S3({apiVersion: '2006-03-01', httpOptions: { timeout: 10 * 60 * 1000 }});
        }
    }

    createEvaluatorTree() {
        this.chunkEvaluatorTree = new Generic(-1, "", "", "", false);
        traverse(this.queryTree, 0, this.chunkEvaluatorTree);
    }

    getHandler(startTime, endTime) {
        var assignedTier = null;
        for (var i = 0; i < this.tiers.length; ++i) {
            var actualRetentionStartTime = Date.now() - this.tiers[i]["retentionStart"] * this.tiers[i]["unit"];
            if (startTime > actualRetentionStartTime) {
                // startTime falls in the range of this tier
                assignedTier =  this.tiers[i]["name"];
                break;
            }
            else if (this.tiers[i]["retentionStart"] == 0) {
                // Found tier with unlimited retention
                assignedTier = this.tiers[i]["name"];
                break;
            }
        }
        // Adjust endTime to the tier's retention end time if needed
        if (this.tiers[i]["retentionEnd"] != 0) {
            var revisedEndTime = (Math.floor(Date.now()/this.unit)*this.unit) - this.tiers[i]["retentionEnd"] * this.tiers[i]["unit"];
            if (endTime > revisedEndTime)
                endTime = revisedEndTime;
        }
        if (assignedTier) {
            this.createEvaluatorTree();
            var handler = createHandler(this.database.Tables[assignedTier].Type, this.chunkEvaluatorTree, assignedTier,
                this.database.Tables[assignedTier].Handle, this.queryText, this.columns, this.timestampColumn,
                startTime, endTime, this.pivot, this.realTime);
            // Since the query refers to the unified table we need the individual handlers to use the unified table
            // as the reference for data.
            handler.setTableAlias(this.table);
            return handler;
        }
        else {
            return null;
        }
    }

    cancel() {
        //console.log("Destroying request");
        if (this.request)
            this.request.abort();
    }

    getQueryObjectFolder() {
        return "queryCache/" + new Date(this.currentStartTime).toISOString().slice(0, 10);
    }

    getQueryHash() {
        var tree = this.evaluatorTree.toJSON();
        var treeJSON = JSON.stringify(tree);
        var resultId = treeJSON + this.currentStartTime + this.currentEndTime;
        var md5Hash = crypto.createHash('md5').update(resultId).digest("hex");

        return md5Hash;
    }

    uploadToS3(objectPath, localFileName) {
        // Gzip the file first
        const gzip = zlib.createGzip();
        const fs = require('fs');
        const inp = fs.createReadStream(localFileName);
        const out = fs.createWriteStream(localFileName + '.gz');

        inp.pipe(gzip).pipe(out).on('finish', () => {

            const gzInp = fs.createReadStream(localFileName + '.gz');
            const bucketParams = {
                Bucket : this.resultCacheBucket,
                Key: objectPath + ".gz",
                Body: gzInp
            };

            var options = { partSize: 5 * 1024 * 1024, queueSize: 10 };  

            this.s3.upload(bucketParams, options, (err, data) => {  
                console.log('Completed ' + err);  
                //fs.unlink(localFileName, () => { console.log("File removed"); });
                fs.unlink(localFileName + '.gz', () => { console.log("File removed"); });
            });   
        });
    }

    persistResultCache() {

        // Save the state
        var state = {};
        state = this.chunkEvaluatorTree.getState(state);
        var stateJson  = JSON.stringify(state);
        var resultCacheJson = JSON.stringify([this.resultCacheHeader, Object.values(this.resultCache)]);

        var md5Hash = this.getQueryHash();
        var stateFile = this.resultCacheFolder + "/" + md5Hash + "_state.json";
        //var resultFile = this.resultCacheFolder + "/" + md5Hash + "_result.json";
        var stateObjectPath = null;
        //var resultObjectPath = null;
        if (this.resultCacheBucket != null) {
            stateObjectPath = this.getQueryObjectFolder() + "/" + md5Hash + "_state.json";
            //resultObjectPath = this.getQueryObjectFolder() + "/" + md5Hash + "_result.json";
        }

        fs.writeFile(this.resultCacheFolder + "/" + md5Hash + "_state.json", stateJson, (err) => { 
            if (err) {
                console.log(err);
            }
            else if (stateObjectPath) {
                this.uploadToS3(stateObjectPath, stateFile);
            }
        });
        /*
        fs.writeFile(this.resultCacheFolder + "/" + md5Hash + "_result.json", resultCacheJson, (err) => { 
            if (err) {
                console.log(err);
            }
            else if (resultObjectPath) {
                this.uploadToS3(resultObjectPath, stateFile);
            }
        });
        */

    }

    updateResultCache(result) {
        if (result[0]["__key__"] == null)
            return;

        this.resultCacheHeader = result[0];
        for (var i=1; i < result.length; ++i) {
            this.resultCache[result[i]["__key__"]] = result[i];
        }
    }
        
    process() {
        var data =  { "__type__": "table"};
        data[this.table] = [];

        if (this.currentEndTime >= this.endTime) {
            /*
            data =  { "__type__": "table"};
            data[this.table] = [];
            if (this.chunkEvaluatorTree) {
                var result = this.chunkEvaluatorTree.flush(data, []);

                if (result && result.length > 1) {
                    this.updateResultCache(result);
                    this.emit("data", this.pivot ? pivotTable(result): result);
                }

                this.persistResultCache();
            }
            */
            this.emit("end");
            return;
        }
        else {
            this.currentStartTime = this.currentEndTime;
            if (this.timeIncrement != 0)
                this.currentEndTime = Math.floor((this.currentStartTime + this.timeIncrement)/this.timeIncrement)*this.timeIncrement;

            if (this.currentEndTime > this.endTime)
                this.currentEndTime = this.endTime;
        }

        // Check if the results can be served from cache
        var md5Hash = this.getQueryHash();


        if (this.resultCacheBucket) {
            var writeStream = fs.createWriteStream(this.resultCacheFolder + "/" + md5Hash + "_state.json");
            var params = {
                Bucket: this.resultCacheBucket, 
                Key: this.getQueryObjectFolder() + "/" + md5Hash + "_state.json.gz"
               };
            this.s3.getObject(params).createReadStream()
                .on('error', (err) => { 
                    // Not in S3 cache
                    // Run the query
                    fs.unlink(this.resultCacheFolder + "/" + md5Hash + "_state.json", () => { });
                    this.processBatch();
                    // console.log('err'); 
                })
                .pipe(zlib.createGunzip()).pipe(writeStream)
                .on('finish', (err) => {
                    var stateJSON = fs.readFileSync(this.resultCacheFolder + "/" + md5Hash + "_state.json");
                    var state = JSON.parse(stateJSON);
                    this.evaluatorTree.loadState(state);
        
                    //var resultJSON = fs.readFileSync(this.resultCacheFolder + "/" + md5Hash + "_result.json");
                    //var result  = JSON.parse(resultJSON);
        
        
                    var data =  { "__type__": "table"};
                    data[this.tableAlias] = [];
        
                    var result = this.evaluatorTree.flush(data, []);
        
                    this.emit('data', result);
                    this.process();
        
                });
            return;
        }
    }

    processBatch() {
        var md5Hash = this.getQueryHash();

        if (fs.existsSync(this.resultCacheFolder + "/" + md5Hash + "_state.json")) /* &&
            fs.existsSync(this.resultCacheFolder + "/" + md5Hash + "_result.json")) */ {
            
            var stateJSON = fs.readFileSync(this.resultCacheFolder + "/" + md5Hash + "_state.json");
            var state = JSON.parse(stateJSON);
            this.evaluatorTree.loadState(state);

            //var resultJSON = fs.readFileSync(this.resultCacheFolder + "/" + md5Hash + "_result.json");
            //var result  = JSON.parse(resultJSON);


            var data =  { "__type__": "table"};
            data[this.tableAlias] = [];

            var result = this.evaluatorTree.flush(data, []);

            this.emit('data', result);
            this.process();
            return;
        }
        var handler = this.getHandler(this.currentStartTime, this.currentEndTime);
        if (handler) {
            handler.on('end', () => {
                this.persistResultCache();

                // Merge this chunk results with the master evaluator tree and flush the results
                var state = this.chunkEvaluatorTree.getState({});
                this.evaluatorTree.loadState(state);
                
                var data =  { "__type__": "table"};
                data[this.tableAlias] = [];
                var result = this.evaluatorTree.flush(data, []);
                this.emit('data', result);
                
                this.process();
            });
            handler.on('data', (result) => {
                //this.updateResultCache(result);
                if (this.realTime || (result[0]["__key__"] == null)) {
                    this.emit('data', result);
                }
            });
            handler.process();
        }
        return handler;

    }

    flush() {
    }
}

function createHandler(type, evaluatorTree, tableName, handle, queryText, columns, timestampColumn, startTime, endTime, pivot, realTime) {
    var handler = null;

    if (type == "solr") {
        handler = new SolrHandler(evaluatorTree, tableName, handle, queryText, 
            columns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    else if (type == "shardedsolr") {
        handler = new ShardedSolrHandler(evaluatorTree, tableName, handle, queryText, 
            columns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    else if (type == "dedupsolr") {
        handler = new DedupSolrHandler(evaluatorTree, tableName, handle, queryText, 
            columns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    else if (type == "file") {
        handler = new FileHandler(evaluatorTree, tableName, handle, 
            columns, pivot, realTime);
    }
    else if (type == "kafka") {
        handler = new KafkaHandler(evaluatorTree, tableName, handle, 
            columns, startTime, endTime, pivot, realTime);
    }
    else if (type == "s3") {
        handler = new S3Handler(evaluatorTree, tableName, handle, queryText, 
            columns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    else if (type == "splunk") {
        handler = new SplunkHandler(evaluatorTree, tableName, handle, queryText, 
            columns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    /*
    else if (type == "unified") {
        handler = new UnifiedHandler(evaluatorTree, db, table.Handle, usedTables[j][1], 
            table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
    }
    */

    return handler;

}

exports.KustoExecutor = class KustoExecutor extends EventEmitter {
    execute(contextConfigStr, database, queryText, timestampColumn, startTime, endTime, pivot, realTime) {
        var contextConfig = JSON.parse(contextConfigStr);
        var tables = [];
        for (var table of Object.keys(contextConfig.Databases[database].Tables)) {
            var columnSchema = contextConfig.Databases[database].Tables[table].OrderedColumns;
            var columnArray = [];
            for (var i=0; i < columnSchema.length; ++i) {
                columnArray.push(new Kusto.Language.Symbols.ColumnSymbol(columnSchema[i].Name, stringToDataType(columnSchema[i].CslType)));
            }
            tables.push(new Kusto.Language.Symbols.TableSymbol.$ctor3(table, columnArray));
        }
        var db = new Kusto.Language.Symbols.DatabaseSymbol(database, tables);

        // For now take the first database as the database on which queyr will be run
        var globals = Kusto.Language.GlobalState.Default.WithDatabase(db);

        var query = Kusto.Language.KustoCode.ParseAndAnalyze(queryText, globals);

        var evaluatorTree = new Generic(-1, "", "", "", false)

        traverse(query.Syntax, 0, evaluatorTree);
        // Get the input tables
        var usedTables = evaluatorTree.getUsedTables([]);
        var outputTables = evaluatorTree.getOutputTables([]);

        for (var j=0; j < outputTables.length; ++j) {
            var handler = null;
            var table = contextConfig.Databases[database].Tables[outputTables[j]];
            if (table.Type == "solr") {
                handler = new SolrHandler(evaluatorTree, outputTables[j], table.Handle, null, 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "shardedsolr") {
                handler = new ShardedSolrHandler(evaluatorTree, outputTables[j], table.Handle, null, 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "file") {
                handler = new FileHandler(evaluatorTree, outputTables[j], table.Handle, 
                    table.OrderedColumns, pivot, realTime);
            }
            else if (table.Type == "kafka") {
                handler = new KafkaHandler(evaluatorTree, outputTables[j], table.Handle, 
                    table.OrderedColumns, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "s3") {
                handler = new S3Handler(evaluatorTree, outputTables[j], table.Handle, null, 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "mongodb") {
                handler = new MongoDBHandler(evaluatorTree, outputTables[j], table.Handle, null, 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            if (handler) {
                evaluatorTree.setOutputHandler(outputTables[j], handler);
            }
        }

        for (var j=0; j < usedTables.length; ++j) {
            var handler = null;
            var table = contextConfig.Databases[database].Tables[usedTables[j][0]];
            if (table.Type == "solr") {
                handler = new SolrHandler(evaluatorTree, usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "shardedsolr") {
                handler = new ShardedSolrHandler(evaluatorTree, usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "dedupsolr") {
                handler = new DedupSolrHandler(evaluatorTree, usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "file") {
                handler = new FileHandler(evaluatorTree, usedTables[j][0], table.Handle, 
                    table.OrderedColumns, pivot, realTime);
            }
            else if (table.Type == "kafka") {
                handler = new KafkaHandler(evaluatorTree, usedTables[j][0], table.Handle, 
                    table.OrderedColumns, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "s3") {
                handler = new S3Handler(evaluatorTree, usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "splunk") {
                handler = new SplunkHandler(evaluatorTree, usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }
            else if (table.Type == "unified") {
                handler = new UnifiedHandler(query.Syntax, evaluatorTree, contextConfig.Databases[database], usedTables[j][0], table.Handle, usedTables[j][1], 
                    table.OrderedColumns, timestampColumn, startTime, endTime, pivot, realTime);
            }

            if (handler) {
                handler.on('end', () => { 
                    this.emit('end');
                });
                handler.on('data', (result) => {

/*                    // Remove internal fields from result
                    // For now only __value__
                    if (result != null) {
                        for (var i=1; i < result.length; ++i) {
                            delete result[i]["__value__"];
                        }
                    }
                    */
                    this.emit('data', result);
                });
                handler.process();
            }
            return handler;
        }
        return null;
    }
}

