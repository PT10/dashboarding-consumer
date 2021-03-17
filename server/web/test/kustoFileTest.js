const Generic = require('../kustoEvaluator.js').generic;
const traverse = require('../kustoEvaluator.js').traverse;
const stringToDataType = require('../kustoEvaluator.js').stringToDataType;
const csv = require('csv-parser');
const fs = require('fs')


function kustoTest(table, columns, file, query) {
    var columnSchema = JSON.parse(columns);
    var columnArray = [];
    for (var key of Object.keys(columnSchema)) {
        columnArray.push(new Kusto.Language.Symbols.ColumnSymbol(key, stringToDataType(columnSchema[key])));
    }
    var database = new Kusto.Language.Symbols.DatabaseSymbol("db", [
            new Kusto.Language.Symbols.TableSymbol.$ctor3(table, columnArray)]);

    var globals = Kusto.Language.GlobalState.Default.WithDatabase(database);

    var query = Kusto.Language.KustoCode.ParseAndAnalyze(query, globals);

    var evaluatorTree = new Generic(-1, "", "", "", false)

    traverse(query.Syntax, 0, evaluatorTree);

    var data =  { "__type__": "table"};
    data[table] = [];

    fs.createReadStream(file)
        .pipe(csv())
    .on('data', (row) => {  

      //debug_log("INPUT: " + JSON.stringify(row));
      // 2007-01-01T00:00:00Z
      //debug_log(Date.parse(row['StartTime']));
      for (var key of Object.keys(columnSchema)) {
          if (columnSchema[key] == 'datetime')
              row[key] = Date.parse(row[key]);
      }
      data[table].push(row);
    
      if (data[table].length == 5000) {
        var output = JSON.stringify(evaluatorTree.evaluate(data, {}));
        //console.log("OUTPUT: " + output);
        data[table] = [];
      }
    
    ////debug_log("OUTPUT: " + JSON.stringify(evaluatorTree.evaluate({"__type__" : "table", "StormEvents" : row}, {})));
  })
  .on('end', () => {
    console.log(new Date());
    var  output = evaluatorTree.evaluate(data, {});
    console.log("OUTPUT: " + JSON.stringify(output, null, 1));
    console.log(new Date());


    console.log(JSON.stringify(output));
  });
  

}

kustoTest(process.argv[2], process.argv[3], process.argv[4], process.argv[5]);
