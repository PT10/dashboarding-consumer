const Generic = require('../kustoEvaluator.js').generic;
const traverse = require('../kustoEvaluator.js').traverse;

function kustoTest() {
    var database = new Kusto.Language.Symbols.DatabaseSymbol("db", [
        new Kusto.Language.Symbols.TableSymbol.$ctor3("T", [
            new Kusto.Language.Symbols.ColumnSymbol("a", Kusto.Language.Symbols.ScalarTypes.Real),
            new Kusto.Language.Symbols.ColumnSymbol("b", Kusto.Language.Symbols.ScalarTypes.Real)
        ])
    ]);

    var globals = Kusto.Language.GlobalState.Default.WithDatabase(database);

    var query = Kusto.Language.KustoCode.ParseAndAnalyze("T | project a = a + b | where a > 10.0", globals);

    var evaluatorTree = new Generic(-1, "", "", "", false)

    traverse(query.Syntax, 0, evaluatorTree);
    var data = { "__type__": "table", "T": [ { "a" : 10, "b" : 15}, { "a" : 3, "b" : 5}  ]  };

    console.log(JSON.stringify(evaluatorTree.evaluate(data, {})));
}

kustoTest();