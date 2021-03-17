const readline = require('readline');
const fs = require('fs');
const kustoExecutor = require('./kustoExecutor.js').KustoExecutor;

var queryConf;
if (process.argv[2].endsWith(".js")) {
    queryConf = require(process.argv[2]).conf;
}
else {
    queryConf = JSON.parse(process.argv[2]);
}
//console.log(JSON.stringify(queryConf));

var sampleQuery;
var sampleRawLogs = [];
var tokenizerResults = [];
var fieldSet = new Set();
var fieldFilters = [];
var fieldDistinctValues = {};
var valueFilterQueries = [];
var downloadFileHandle;
var downloadFileSize = 0;
var tokenizerLimit = 1;

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

var tokenizerQuery = "";
var filterQuery = "";
var valueFilterQueryClause = "";

const database = process.argv[3];
const table = process.argv[4];
const logField = process.argv[6];
const tableFile = table + "_file";
const tableFileName = table + "_file.json";

// Create a separate queryConf for downloaded file
var fileQueryConf = { Databases : { }};
fileQueryConf.Databases[database] =  
    { Name: database,
      Functions : {},
      Tables: {
      }
    };
fileQueryConf.Databases[database].Tables[tableFile] = {};
fileQueryConf.Databases[database].Tables[tableFile].OrderedColumns = queryConf.Databases[database].Tables[table].OrderedColumns;
fileQueryConf.Databases[database].Tables[tableFile].Name = tableFile;
fileQueryConf.Databases[database].Tables[tableFile].Type = "file";
fileQueryConf.Databases[database].Tables[tableFile].Handle = tableFileName;

function processRawLog() {
    rl.question("Download sample logs? [y/n] ", (response) => {
        if (response == 'y') {
            rl.question("Enter query to download sample logs: ", (query) => {   
                sampleQuery = query;
                console.log("Downloading sample logs ...")
                var tempQuery = query + " | limit 5";
                executeKustoQuery(queryConf, tempQuery, sampleQueryData, sampleQueryEnd);
            });
        }
        else {
            if (fs.existsSync(tableFileName)) {
                console.log("Proceeding with cached raw logs ...")
                processTokenizeQuery();
            }
            else {
                process.exit();
            }
        }    
    });
}

function sampleQueryData(result) {
    result.shift();
    sampleRawLogs.push(result);
}

function sampleQueryEnd() {
    console.log("Sample logs: ");
    console.log(JSON.stringify(sampleRawLogs, null, 2));

    rl.question("Download entire sample set for analysis ? [y/n/q] ", (response) => {
        if (response == 'y') {
            processDownload();
        }
        else if (response == 'n') {
            if (fs.existsSync(tableFileName)) {
                console.log("Proceeding with cached raw logs ...")
                processTokenizeQuery();
            }
            else {
                console.log("Cached raw log file not found.");
                process.exit();
            }
        }
        else {
            process.exit();
        }
    });
}

function processDownload() {
    downloadFileHandle = fs.openSync(tableFileName, "w");
    executeKustoQuery(queryConf, sampleQuery, downloadData, downloadEnd);
}

function downloadData(result) {
    for (var j=1; j < result.length; ++j) {
        fs.writeSync(downloadFileHandle, JSON.stringify(result[j]));
        fs.writeSync(downloadFileHandle, "\n");
        if (result[j][logField])
            downloadFileSize += result[j][logField].length;
    }
}

function downloadEnd() {
    fs.closeSync(downloadFileHandle);
    var f = fs.openSync(table + "_size.txt", "w");
    fs.writeSync(f, ""+downloadFileSize);
    fs.closeSync(f);
    processTokenizeQuery(1);
}

function processTokenizeQuery() {
    if (tokenizerLimit == 0) {
        var tempQuery = tableFile + " | " + tokenizerQuery;
        executeKustoQuery(fileQueryConf, tempQuery, tokenizerData, tokenizerEnd);
    }
    else {
        console.log("\n\n-------------------------------\n\n")
        rl.question('Tokenization query ==> ', (query) => {

            tokenizerQuery = query;

            // Add limit of 1 to query so that results are small
            var tempQuery = tableFile + " | " + query + " | limit " + tokenizerLimit;
            executeKustoQuery(fileQueryConf, tempQuery, tokenizerData, tokenizerEnd);
        });
    }
}

function tokenizerData(results) {
    var fields = results[0]["__header__"];
    for (var i=0; i < fields.length; ++i) {
        fieldSet.add(fields[i]["name"]);
    }
    // Don't accumulate results when entire dataset is being tokenized
    if (tokenizerLimit != 0) {
        results.shift();
        tokenizerResults.push(results);
    }
}

function tokenizerEnd() {
    if (tokenizerLimit != 0) {
        console.log("Tokenizer results: ");
        console.log(stringify(tokenizerResults, null, 2));
        
        rl.question('Sample Tokenizer results OK? [y/n] ', (response) => {
            if (response == 'y') {
                tokenizerLimit = 0;
                // Rerun tokenizer on entire data now
                console.log("Running tokenization on entire data ...")
                processTokenizeQuery();
            }
            else {
                processTokenizeQuery();
            }
        });
    }
    else {
        processDistinctValues();        
    }
}

function processDistinctValues() {
    console.log("Estimating distinct values and space occupied for each field ... ")
    // Detecting field unique value counts
    // Rerun original query but this time but calculate distinct count for each unique value
    // Create summarize dcount query
    var dCountQuery = "";
    for (var item of fieldSet) {
        if (dCountQuery != "")
            dCountQuery += ", ";
        dCountQuery += (item + "_count=dcount(['" + item + "']), " + item + "_mb=sum(toreal(strlen(['" + item + "']))/1024/1024)");
    }
    dCountQuery = tableFile + " | " + tokenizerQuery + "| summarize " + dCountQuery;
    executeKustoQuery(fileQueryConf, dCountQuery, dCountData, dCountEnd);
}

function dCountData(result) {
    if (!result)
        return;
    for (var i=1; i < result.length; ++i) {
        for (var key of Object.keys(result[i])) {
            // Ignore internal fields
            if (key.startsWith("__"))
                continue;
            else if (key.endsWith("_count")) {
                var field = key.substring(0, key.length-6);
                if (fieldDistinctValues[field] == null) {
                    fieldDistinctValues[field] = {};
                }
                fieldDistinctValues[field]["count"] = result[i][key];
            }
            else if (key.endsWith("_mb")) {
                var field = key.substring(0, key.length-3);
                if (fieldDistinctValues[field] == null) {
                    fieldDistinctValues[field] = {};
                }
                fieldDistinctValues[field]["mb"] = result[i][key];
            }
        }
    }
}

function dCountEnd() {
    var totalMb=0;
    for (var field of Object.keys(fieldDistinctValues)) {
        totalMb += fieldDistinctValues[field]["mb"];
    }

    // Calculate Percent usage
    for (var field of Object.keys(fieldDistinctValues)) {
        fieldDistinctValues[field]["percent"] = fieldDistinctValues[field]["mb"]*100/totalMb;
        //delete fieldDistinctValues[field]["mb"];
    }
    //var stats = fs.statSync(tableFileName);
    downloadFileSize = parseInt(fs.readFileSync(table +"_size.txt"), "r")/(1024*1024);
    //downloadFileSize = stats.size / (1024*1024);

    console.log("\n\n-------------------------------\n\n")
    console.log("Raw log file size (MB) : " + downloadFileSize);
    console.log("Log file size after tokenization (MB) : " + totalMb);
    console.log("Percentage reduction after tokenization : " + ((downloadFileSize-totalMb)*100.0)/downloadFileSize);
    console.log("\n");
    console.log("Field distinct value counts and space occupied in (%) are : " );
    console.log(stringify(fieldDistinctValues, null, 2));
    console.log("\n\n-------------------------------\n\n")
    console.log("Fields with low distinct values are possible candidates for removal.");
    console.log("Fields with high space occupied percntage are possible candidates for removal or replaced with a smaller size value substitution.");
    console.log("\n\n-------------------------------\n\n")
    processFieldFilters();
}

function processFieldFilters() {
    rl.question('Specify field to be removed (comma separated) ==> ', (filters) => {
        fieldFilters = filters.split(/[,\s]+/);
        var totalMb=0;
        for (var field of Object.keys(fieldDistinctValues)) {
            if (fieldFilters.includes(field)) {
                fieldSet.delete(field);
                continue;
            }
            totalMb += fieldDistinctValues[field]["mb"];
        }
        console.log("Log file size after tokenization and field removals (MB) : " + totalMb);
        console.log("Percentage reduction after tokenization and field removals :  " + ((downloadFileSize-totalMb)*100.0)/downloadFileSize);
        processValueFilters();
    });
}

function processValueFilters() {
    rl.question('Add field value filters (field name followed by value to be filtered). Enter when done. ==> ', (filter) => {
        var fieldName = filter.substring(0, filter.indexOf(" "));
        var valueFilter = filter.substring(filter.indexOf(" ")+1, filter.length);
        if (filter != '') {
            if (fieldName != '' && valueFilter != '')
                valueFilterQueries.push([fieldName, valueFilter]);
                processValueFilters();
        }
        else {
            console.log("Estimating reduction ...")
            // Construct value filter query
            for (var i=0; i < valueFilterQueries.length; ++i) {
                valueFilterQueryClause += "| where " + valueFilterQueries[i][0] + " !contains '" + valueFilterQueries[i][1] + "'";
            }
            // Create summarize dcount query
            var dCountQuery = "";
            for (var item of fieldSet) {
                if (dCountQuery != "")
                    dCountQuery += ", ";
                dCountQuery += (item + "_mb=sum(toreal(strlen(['" + item + "']))/1024/1024)");
            }
            dCountQuery = tableFile + " | " + tokenizerQuery + valueFilterQueryClause + " | summarize " + dCountQuery;
            fieldDistinctValues = {};
            executeKustoQuery(fileQueryConf, dCountQuery, dCountData, processScheduleQuery);        
        }
    });
}


function processScheduleQuery() {
    var totalMb=0;
    for (var field of Object.keys(fieldDistinctValues)) {
        if (fieldDistinctValues[field]["mb"])
            totalMb += fieldDistinctValues[field]["mb"];
    }

    console.log("\n\n-------------------------------\n\n")
    console.log("Raw log file size (MB) : " + downloadFileSize);
    console.log("Log file size after tokenization, field removal and value filter  (MB) : " + totalMb);
    console.log("Percentage reduction after tokenization, field removal and value filter : " + ((downloadFileSize-totalMb)*100.0)/downloadFileSize);
    console.log("\n");

    console.log("Query : " + tokenizerQuery);
    console.log("Tokenized fields: " + Array.from(fieldSet));
    console.log("Fields to be removed: " + fieldFilters);
    console.log("Field values to be filtered: " + stringify(valueFilterQueries));
    console.log("\n\n-------------------------------\n\n")

    rl.question('Schedule this? [y/n/q]' , (answer) => {
        if (answer == 'y') {
            console.log("Done");
            process.exit();
        }
        else if (answer == 'q') {
            process.exit();
        }
        else {
            // Start over
            processTokenizeQuery();
        }
    });
}

function stringify(obj) {
    return JSON.stringify(obj, function(key, val) {
        return val.toFixed ? Number(val.toFixed(2)) : val;
    }, 2);
}

function executeKustoQuery(queryConf, query, cbData, cbEnd) {
    //console.log("Executing query: " + query);
    var ex = new kustoExecutor();
    if (cbData != null) {
        ex.on("data", cbData);
    }
    if (cbEnd != null)
        ex.on("end", cbEnd);
    
    ex.execute(JSON.stringify(queryConf), process.argv[3], query, process.argv[6], process.argv[7], process.argv[8], false, false);
}

processRawLog();

//processTokenizeQuery();
