const bridge = require('./kustolib/bridge.js');
const kusto = require('./kustolib/Kusto.Language.Bridge.js');
var solr = require('solr-node');

var debug = false;
function debug_log(msg) {
    if (debug) {
        console.log(msg);
    }
}

var options = {};

exports.stringToDataType = function stringToDataType(dataType) {
    switch (dataType) {
        case "real":
            return Kusto.Language.Symbols.ScalarTypes.Real;
        case "bool":
            return Kusto.Language.Symbols.ScalarTypes.Bool;
        case "long":
            return Kusto.Language.Symbols.ScalarTypes.Long;
        case "int":
            return Kusto.Language.Symbols.ScalarTypes.Int;
        case "decimal":
            return Kusto.Language.Symbols.ScalarTypes.Decimal;
        case "datetime":
            return Kusto.Language.Symbols.ScalarTypes.DateTime;
        case "timespan":
            return Kusto.Language.Symbols.ScalarTypes.TimeSpan;
        case "dynamic":
            return Kusto.Language.Symbols.ScalarTypes.Dynamic;
        case "guid":
            return Kusto.Language.Symbols.ScalarTypes.Guid;
        case "string":
            return Kusto.Language.Symbols.ScalarTypes.String;
        case "type":
            return Kusto.Language.Symbols.ScalarTypes.Type;
        default:
            return null;
    }
}

function string_of_enum(enum_value) {
  for (const [key, value] of Object.entries(Kusto.Language.Syntax.SyntaxKind)) {
      if (value == enum_value) 
          return key;
  }
  return null;
}

function datatypeToString(dataType) {
    if (!dataType)
        return null;

    if (dataType.Kind == Kusto.Language.Symbols.SymbolKind.Table) {
        // Return all the column name and type has a dictionary
        var columns = dataType.Columns.Items._items;
        var r = [];
        var i;
        for (i=0; i < columns.length; ++i) {
            var c = {};
            c['name'] = columns[i].Name;
            c['type'] = datatypeToString(columns[i].Type);
            r.push(c);
            //r[columns[i].Name] = datatypeToString(columns[i].Type)
        }
        return r;
    }
    // Else assume scalar type for now.
    switch (dataType) {
        case Kusto.Language.Symbols.ScalarTypes.Real:
            return "real";
        case Kusto.Language.Symbols.ScalarTypes.Bool:
            return "bool";
        case Kusto.Language.Symbols.ScalarTypes.Long:
            return "long";
        case Kusto.Language.Symbols.ScalarTypes.Int:
            return "int";
        case Kusto.Language.Symbols.ScalarTypes.Decimal:
            return "decimal";
        case Kusto.Language.Symbols.ScalarTypes.DateTime:
            return "datetime";
        case Kusto.Language.Symbols.ScalarTypes.TimeSpan:
            return "timespan";
        case Kusto.Language.Symbols.ScalarTypes.Dynamic:
            return "dynamic";
        case Kusto.Language.Symbols.ScalarTypes.Guid:
            return "guid";
        case Kusto.Language.Symbols.ScalarTypes.String:
            return "string";
        case Kusto.Language.Symbols.ScalarTypes.Type:
            return "type";
        default:
            return "Unknown : " + dataType.name;
    }
}

exports.traverse = function traverse(node, level, currentTree, tablesUsed) {
    var gen = exports.generic;
    var i;
    if (!node) {
        evaluatorObject = new gen(-1, null, null, null, null, null, null, null);
        currentTree.children.push(evaluatorObject);
        return;
    }
    kindInt = node.Kind;
    kind = string_of_enum(node.Kind);
    text = node.toString(Kusto.Language.Syntax.IncludeTrivia.Minimal);
    simpleName = node.SimpleName;
    literalValue = node.LiteralValue;
    isLiteral = node.IsLiteral;
    resultType = datatypeToString(node.ResultType);
    if (node.ResultType && node.ResultType.Kind == Kusto.Language.Symbols.SymbolKind.Table)
        isTable = true;
    else
        isTable = false;

    //console.log("Node type" + node.ResultType);

    if (kind == 'CustomNode') {
        // Create evaluator object for the specific command instead of geneirc CustomNode
        kind = string_of_enum(node.GetChild(0).Kind);
        kind = kind + "_CustomNode";
    }
    const evaluatorClass = classMap[kind];

    var evaluatorObject;

    if (evaluatorClass != null) {
        evaluatorObject = new evaluatorClass(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType, isTable);
    }
    else {
        evaluatorObject = new gen(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType, isTable);
    }
    currentTree.children.push(evaluatorObject);

    for (i=0; i < node.ChildCount; ++i) {
        child = node.GetChild(i);
        traverse(child, level+1, evaluatorObject);
    }
}

exports.generic = class Generic {
    timeTaken = 0;
    evalStart = 0;
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType, isTable) {
        this.kindInt = kindInt;
        this.kind = kind;
        this.text = text;
        this.simpleName = simpleName;
        this.literalValue = literalValue;
        this.isLiteral = isLiteral;
        this.resultType = resultType;
        this.isTable = false;
        if (isTable == true)
            this.isTable = true;
        this.children = [];
        this.resultTypeMap = { "__header__" : this.resultType };
    }

    begin() {
        this.evalStart = Date.now();
    }

    end() {
        this.timeTaken += (Date.now()-this.evalStart);
    }

    printStats() {
        console.log("Time taken by " + this.kind + " " + this.text + " " + this.timeTaken);
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].printStats();
        }      
    }

    addChild(node) {
        this.children.push(node);
    }

    evaluate(context, result) {
        var i;
        // Default is to evaluate each of the children
        for (i=0; i < this.children.length; ++i) {
            result = this.children[i].evaluate(context, result);
        }
        return result;
    }

    getUsedTables(usedTables) {
        for (var i=0; i < this.children.length; ++i) {
            usedTables = this.children[i].getUsedTables(usedTables);
        }
        return usedTables;
    }
    
    setOutputHandler(table, handler) {
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].setOutputHandler(table, handler);
        }
    }

    getOutputTables(outputTables) {
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].getOutputTables(outputTables);
        }
        return outputTables;
    }

    getResultTypeWithColumnName() {
        if (this.resultType == null) {
            var i;
            for (i=0; i < this.children.length; ++i) {
                if (this.children[i].resultType != null)
                    return this.children[i].getResultTypeWithColumnName();
            }
        }
        else {
            return this.resultType;
        }
    }

    init() {
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].init();
        }
    }

    flush(context, result) {
        for (var i=0; i < this.children.length; ++i) {
            // Evaluate in null context
            this.children[i].evaluate(context, result);
            result = this.children[i].flush(context, result);
        }
        return result;
    }

    getState(state) {
        state['text'] = this.text;
        state['state'] = [];
        for (var i=0; i < this.children.length; ++i) {
            state['state'].push(this.children[i].getState({}));
        }
        return state;
    }

    loadState(state) {
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].loadState(state['state'][i]);
        }
    }

    toJSON() {
        var json = [this.kindInt, this.simpleName, this.literalValue, []];
        for (var i=0; i < this.children.length; ++i) {
            json[3].push(this.children[i].toJSON());
        }        
        return json;
    }
}

class SetOptionStatement extends   exports.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        // First element is set keyworkd
        // Second elemnt is NameDeclaration - name of the option
        // Third element is OptionValueClause - option value clause 
        // First element is inside option value clause is '=' token
        // Second element is option value

        var optionValue = this.children[2].children[1].evaluate(context, result);
        options[this.children[1].simpleName] = optionValue[1];
        return result;
    }
}

class Export extends   exports.generic {
    handler = null;
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    getUsedTables(usedTables) {
        // Eighth element is the remainder query
        return this.children[7].getUsedTables(usedTables);
    }
    
    setOutputHandler(table, handler) {
        // First element is 'export'
        // Second element is optional 'async'
        // Third element is 'to'
        // Fourth element is 'table'
        // Fifth element is NameReference to the table to be exported to
        // Sixth element is empty (unknown)
        // Seventh element is <|
        // Eighth element is the remainder (query)
        if (this.children[4].text.trim() == table) {
            this.handler = handler;
        }
    }

    getOutputTables(outputTables) {
        outputTables.push(this.children[4].text.trim());
        return outputTables;       
    }
    evaluate(context, result) {
        // Evaluate the query first and then pass the results to the handler to output
        if (this.children[7] == null) {
            console.log("NULL");
        }
        result = this.children[7].evaluate(context, result);
        this.handler.update(result);
        return {}; // Return empty result for now
    }

    flush(context, handler) {
        this.handler.flush();
    }
    
}

class EvaluatePlugin {
    constructor() {
    }
    getUsedTables(usedTables, params) {
        return usedTables;
    }

}
class BagUnpackPlugin extends EvaluatePlugin {
    constructor() {
        super();
    }

    evaluate(pluginArgs, context, result, params) {
        var results;
        var evaluateResults = [];
        // For now empty header
        var header = { "__header__" : [ ], "__key__" : [ ]};
        var columns = {};

        for (var j=1; j < result.length; ++j) {
            var params = [];

            var columnValue = pluginArgs[0].evaluate(context, result[j]);
            for (var key of Object.keys(columnValue[1])) {
                var value = columnValue[1][key];
                if (columns[key] != "dynamic") {
                    var newType;
                    if(typeof value == "number") {
                        newType = "real";
                    }
                    else if (typeof value == "string") {
                        if (isNaN(value)) {
                            newType = "string";
                        }
                        else {
                            var newVal = parseFloat(value);
                            if (parseFloat(value)% 1 > 0) {
                                newType = "real";
                                columnValue[1][key] = newVal;
                            }
                            else {
                                newType = "long";
                                columnValue[1][key] = Math.floor(newVal);
                            }
                        }
                    }
                    else { 
                        newType = "dynamic";
                    }
                    if (columns[key] == null) {
                        columns[key] = newType;
                    }
                    else if (columns[key] != newType) {
                        columns[key] = "dynamic";
                    }
                }
            }

            // First parameter of plugin is the column name contents to be unpacked
            // As of now just return it
            evaluateResults.push(columnValue[1]);
        }
        for (var key of Object.keys(columns)) {
            header["__header__"].push({ "name" : key, "type" : columns[key]});
        }
        evaluateResults.unshift(header);
        return evaluateResults;
    }
}

class SearchIndexPlugin extends EvaluatePlugin {
    collection;
    constructor() {
        super();
        this.batchSize = 1;
        this.currentCursor = 0;
        this.numFound = 0;
        this.collection = null;
    }

    evaluate(context, result, params) {
        // Data is will coming through the context
        // Just return it back
        if (context[this.collection] != null) {
            result = context[this.collection];
        }
        return result;
    }

    /*** NO CUSTOM EVALUATION 
    evaluate(context, result, params) {
        var collection = params[0][1];
        var queryText = params[1][1];

        if (this.currentCursor > this.numFound)
            return null;
        
        // Create client
        var client = new solr({
            host: 'localhost',
            port: '8983',
            core: collection,
            protocol: 'http'
        });

        var query = client.query()
            .q(queryText)
            .addParams({
                wt: 'json',
                indent: false
            }).start(this.currentCursor).rows(this.batchSize);

        var resultTable = null;
        var response = null;
        (async () => {
            response = await client.search(query).then(function(result) {
                    return result;
            });
        });
        this.numFound = response.response.numFound;
        resultTable = response.response.docs;
        return resultTable;
    }
    ****/

    getUsedTables(usedTables, params) {
        // Two parameters expected. Table name and the query string
        var tempUsedTable = [];
        tempUsedTable = params[0].getUsedTables(tempUsedTable);

        // Save the collection for later evaluation
        this.collection = tempUsedTable[0][0];
        // Evaluate the query string to get back value
        var queryStringValue = params[1].evaluate({}, {});
        tempUsedTable[0][1] = queryStringValue[1];
        usedTables.push(tempUsedTable[0]);
        return usedTables;
    }
}

class EvaluateOperator extends this.generic {
    pluginObject = null;

    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
        this.pluginObject = null;
    }

    evaluate(context, result) {
        // First is evaluate keyword
        // Second empty list - might be plugin parameters
        // Third is FunctionCallExpression
        // Inside first is the function name (plugin name in this case)
        // And then an expression list.
        var pluginName = this.children[2].children[0].simpleName;

        if (evaluatePluginMap[pluginName])
            this.pluginObject = new evaluatePluginMap[pluginName];
        if (this.pluginObject == null)
            return result;

        // Evaluate the expression list and then call the Plugin object's evaluate method
        var expressionList = this.children[2].children[1];

        return this.pluginObject.evaluate(expressionList.children[1].children, context, result);

    }

    getUsedTables(usedTables) {
        var pluginName = this.children[2].children[0].simpleName;

        if (evaluatePluginMap[pluginName])
            this.pluginObject = new evaluatePluginMap[pluginName]();
        else
            return usedTables;
        // Evaluate the expression list and then call the Plugin object's evaluate method
        var params = [];
        var expressionList = this.children[2].children[1];

        // Inside expression list, is open param token and list of expressions and end param token
        for (var i=0; i < expressionList.children[1].children.length; ++i) {
            var expression = expressionList.children[1].children[i];
            if (expression.kind == 'CommaToken') {
                continue;
            }
            else {
                params.push(expression);
            }
        }
        return this.pluginObject.getUsedTables(usedTables, params);
    }
}

class SearchOperator extends this.generic {

    // As of now assume search is evaluated externally
    isExternallyEvaluated = true;

    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        // Data is will coming through the context
        // Just return it back
        var resultTable = [];

        // Flatten each of the tables in contex and add '$table' as new column
        // This is as per search operator spec
        for (var key of Object.keys(context)) {
            if (key == "__type__")
                continue;

            for (var i=0; i < context[key].length; ++i) {
                context[key][i]['$table'] = key;
                resultTable.push(context[key][i]);
            }
        }
        resultTable.unshift(this.resultTypeMap);
        return resultTable;
    }

    getUsedTables(usedTables) {
        // First is search keyword
        // Second is empty list (probably parameters)
        // Third is FindInClause
        // Fourth is SearchExpression

        // Descend into FindInClause and get the tables
            // First is in keyword
            // Second is '('
            // Third is empty (TBD)
            // Fourth is list of tables. Descend into this list to get the tables
        var findInClause = this.children[3];
        for (var i=0; i < findInClause.children[2].children.length; ++i) {
            var table = findInClause.children[2].children[i];
            if (table.kind == 'CommaToken') {
                continue;
            }
            else {
                usedTables = table.getUsedTables(usedTables)
            }
        }

        // As of now just take the Search Expression assume and assume it is a LiteralExpression in Solr syntax
        var query = this.children[4].literalValue;
        for (var i=0; i < usedTables.length; ++i) {
            usedTables[i][1] = query;
        }
        return usedTables;
    }
}


class TakeOperator extends this.generic {
    takeLimit = 0;
    currentCount = 0;

    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);


    }

    evaluate(context, result) {
        ////debug_log("Take operator " + JSON.stringify(context));
        var j;
        var resultTable = [];
        // Take operator's children has 'limit/take' keyword, an empty list (not sure what that is), follow by the limit/take value

        this.takeLimit = this.children[2].evaluate(context, result)[1];

        if (this.currentCount >= this.takeLimit)
        return null;

        for (j=1; j < result.length; ++j) {
            //debug_log("Take operator " + JSON.stringify(result[j]));
            if (this.currentCount < this.takeLimit) {
                this.currentCount += 1;
                resultTable.push(result[j]);
            }
        }
        resultTable.unshift(result[0]);
        return resultTable;
    }
}

class CountOperator extends this.generic {
    uniqueSet = new Set();
    currentCount = 0;

    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        ////debug_log("Count operator " + JSON.stringify(context));
        var j;
        for (j=1; j < result.length; ++j) {
            //debug_log("Count operator " + JSON.stringify(result[j]));
            if (result[j]["__key__"] != null) {
                this.uniqueSet.add(result[j]["__key__"]);
            }
            else {
                // Just make up a new unique key
                this.uniqueSet.add("__key__" + this.currentCount);
                this.currentCount += 1;
            }
        }
        return [{ "__header__" : [ {"name" : "count", "type" :"long" }], "__key__" : [ ]}, { "__key__" : "count", "count" : this.uniqueSet.size}];
    }
}

class ProjectOperator extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        this.begin();
        var projectResults = [];
        //debug_log("Project operator context " + JSON.stringify(context));
        var j;
        for (j=1; j < result.length; ++j) {
            //debug_log("Project operator result " + JSON.stringify(result[j]));
            // Expect two children: "project" keyword and list of project expressions
            var i;
            var projections = this.children[1];
            var projectResult = {};
            for (i=0; i < projections.children.length; ++i) {
                // Note the change in context below
                var temp = projections.children[i].evaluate(context, result[j]);
                if (Array.isArray(temp)) {
                    projectResult[temp[0]] = temp[1];
                }else {
                    projectResult = {  ... projectResult, ... temp, };
                }
            }
            projectResults.push(projectResult);
        }
        //result[0] = { ... result[0], ... this.resultTypeMap };
        projectResults.unshift(this.resultTypeMap);
        this.end();
        return projectResults;
    }
}

class ProjectRenameOperator extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        this.begin();
        //debug_log("Project operator context " + JSON.stringify(context));
        var j;
        for (j=1; j < result.length; ++j) {
            var renameList = this.children[1].children;
            for (var k=0; k < renameList.length; ++k) {
                var expression = renameList[k].children[0];
                var newName = expression.children[0].simpleName;
                // In the middle is '=' token
                var oldName = expression.children[2].simpleName;
                result[j][newName] = result[j][oldName];
                delete result[j][oldName];
            }
        }
        //result[0] = { ... result[0], ... this.resultTypeMap };
        result[0] = this.resultTypeMap;
        this.end();
        return result;
    }
}

class ExtendOperator extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        var begin = Date.now();
        var resultTable = [];
        var j;
        for (j=1; j < result.length; ++j) {
        // Expect two children: "extend" keyword and list of extend expressions
        var i;
            var extendss = this.children[1];
            for (i=0; i < extendss.children.length; ++i) {
                var temp = extendss.children[i].evaluate(context, result[j]);
                if (Array.isArray(temp)) {
                    result[j][temp[0]] = temp[1];
                }else {
                    result[j] = { ... result[j], ... temp };
                }
            }
        }
        //result[0] = { ... result[0], ... this.resultTypeMap };
        // Result type map of extend contains previous columns as well new columns added by extend operator
        result[0] = this.resultTypeMap;
        debug_log(this.kind + " " + this.text + " took " + (Date.now() - begin));
        return result;
    }
}


class FilterOperator  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var begin = Date.now();
        var resultTable = [];
        var j;
        for (j=1; j < result.length; ++j) {
            // Expect two children ideally: "where" keyword and predicate but I am seeing an empty list in between
            var predicate = this.children[2];
            var predicateResult = predicate.evaluate(context, result[j]);
            if (predicateResult[1]) {
                resultTable.push(result[j]);
            }
        }

        // Filter operator doesn't add or change data types
        resultTable.unshift(result[0]);
        debug_log(this.kind + " took " + (Date.now() - begin));
        return resultTable;
    }
}




class MvApplyOperator  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        this.begin();
        var resultTable = [];
        var j;
        for (j=1; j < result.length; ++j) {
            // mv-apply pairs on ( summarize bag=make_bag(pack(tostring(pairs[0]), pairs[1])))
            // First element is mv-apply keyword
            // Second element is ItemIndex (optional)
            // Third element is list of column array expressions to be expanded of type MvApplyExpression
            // Fourth is row limit caluse of type MvApplyRowLimitClause
            // Fifth is empty (not known)
            // Sixth element is on keyword
            // Seventh element is the subquery (type MvApplySubqueryExpression)  
            var columnsToExpand = this.children[2];
            // Evaluate each column and merge the results into a unified array to be sent to sub query
            var expandedResult = [];
            for (var k=0; k < columnsToExpand.children.length; ++k) {
                var columnResult = columnsToExpand.children[k].evaluate(context, result[j]);
                var columnName = columnsToExpand.children[k].text.trim();
                // Column expression could be a bare variable e.g. pairs or a named expression element=pairs
                // In the second case use the LHS as the column name to be passed to subquery
                var equalLoc = columnName.indexOf('=');
                if (equalLoc > 0)
                    columnName = columnName.substr(0, equalLoc);

                if (columnResult[1] == null)
                    continue;

                for (var l=0; l < columnResult[1].length; ++l) {
                    if (l >= expandedResult.length)
                        expandedResult[l] = {}
                    
                    expandedResult[l][columnName] = columnResult[1][l];
                }
            }

            // The context for subquery change to the expanded result
            expandedResult.unshift({}); // Added Empty header
            var subqueryIndex = 4;
            while (this.children[subqueryIndex].kind != 'MvApplySubqueryExpression') {
                subqueryIndex++;
            }

            // Initialize the evaluator as each row needs to be freshly evaluated independent of previous rows
            this.children[subqueryIndex].init();
            var subqueryResults = this.children[subqueryIndex].evaluate(context, expandedResult);
            for (var k=1; k < subqueryResults.length; ++k) {
                resultTable.push(subqueryResults[k]);
            }
        }

        // Filter operator doesn't add or change data types
        resultTable.unshift(result[0]);
        this.end();
        return resultTable;
    }
}
class SummarizeOperator  extends this.generic {
    summarizedResults = {};
    emitHistory = {};
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
        /*
        // Create empty dictionary for each aggregate expression
        var i = 0;
        for (i=0; i < this.children[2].children.length; ++i) {
            this.summarizedResults.push({});
        }
        */
    }

    init() {
        this.summarizedResults = {};
        this.emitHistory = {};

        // Clear out cache of summary functions
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].init();
        }
    }

    getState(state) {
        state['state'] = [];
        state['text'] = this.text;

        for (var i=0; i < this.children.length; ++i) {
            state['state'].push(this.children[i].getState({}));
        }

        state['summarizedResults'] = this.summarizedResults;
        return state;
    }

    loadState(state) {
        this.summarizedResults = state['summarizedResults'];
        for (var i=0; i < this.children.length; ++i) {
            this.children[i].loadState(state['state'][i]);
        }

        for (var byResultKey of Object.keys(this.summarizedResults)) {
            var byResult = {};
            byResult["__key__"] = byResultKey;
            for (var i=0; i < this.children[2].children.length; ++i) {
                var aggregate = this.children[2].children[i];
                if (aggregate.kind == 'CommaToken') {
                    continue;
                }
                // Note the byResult becomes context for the aggregate expression evaluation
                // byResult contains original columns and columns newly added by the by clause
                //var aggregateResult = aggregate.evaluate(byResult, this.summarizedResults[byResultKey][i]);
                var aggregateResult = aggregate.evaluate(byResult, null);
                // If it was a Simple Named Expression then the new column will come back in array itself
                // Other it will be tuple returned by Function Call Expression
                //debug_log("Aggregate result is " + JSON.stringify(aggregateResult));
                if (Array.isArray(aggregateResult)) {
                    this.summarizedResults[byResultKey][i][aggregateResult[0]] = aggregateResult[1];
                }
                else {
                    this.summarizedResults[byResultKey][i] = { ... this.summarizedResults[byResultKey][i],  ... aggregateResult }
                }
            }
        }
    }
    
    evaluate(context, result) {
        this.begin();
        var begin = Date.now();

        var j;
        var changedKeys = new Set();
        for (j=1; j < result.length; ++j) {  
            // First child is 'summarize' token. Followed by empty list (not sure what it is)
            // Followed by List of aggregate expressions

            // Next is the 'by' clause

            // First the by clause will be processed
            // The 'by' clause can be thought of new temporary "extends" for the purpose of aggregation
            // So we will process each by clause like any other expression and let it add new columns when required
            // In the result 'by' clause evaluator will return the list of 'by' columns.
            var byClause = this.children[3];
            // Switch context for by clause evaluation to incoming result because now we are inside row now
            var byClauseResult = {};
            if (byClause && byClause.kind != null)
                byClauseResult = byClause.evaluate(result[j], result[j]);
            //debug_log("By Clause result is " + JSON.stringify(byClauseResult));

            // Merge incoming results with new/updated columns created by by clause
            var byResult = { ... result[j], ... byClauseResult};
            //byResult["__type__"] = "table";

            if (j == 1) {
                this.resultTypeMap["__key__"] = Object.keys(byClauseResult);
            } 
            var byResultKey = Object.values(byClauseResult).join();
            changedKeys.add(byResultKey);

            // Add it to byResult so that downstream aggregate operators can use it
            byResult["__key__"] = byResultKey;

            if (this.emitHistory[byResultKey] == null) {
                this.emitHistory[byResultKey] = { lastEmitTime : Date.now(), lastUpdateTime : Date.now()};
            }
            else {
                this.emitHistory[byResultKey]["lastUpdateTime"] = Date.now();
            }

            if (!this.summarizedResults[byResultKey]) {
                // Create a place holder for this new byClause key
                this.summarizedResults[byResultKey] = [] // byClauseResult;
                // Create a separate place holder inside for each aggregate expression
                var i;
                for (i=0; i < this.children[2].children.length; ++i) {
                    var aggregate = this.children[2].children[i];
                    if (aggregate.kind == 'CommaToken') {
                        // Push an empty dict so that we can access this array just using index later
                        this.summarizedResults[byResultKey].push({});
                        continue;
                    }
                    else {
                        // Note: Make a separate copy of byClauseResult for each aggregate
                        // Otherwise they will overwrite each other
                        this.summarizedResults[byResultKey].push({ ... byClauseResult});
                    }
                }   
            }

            // Now process each of the aggregate expression
            var i;
            for (i=0; i < this.children[2].children.length; ++i) {
                var aggregate = this.children[2].children[i];
                if (aggregate.kind == 'CommaToken') {
                    continue;
                }
                // Note the byResult becomes context for the aggregate expression evaluation
                // byResult contains original columns and columns newly added by the by clause
                var aggregateResult = aggregate.evaluate(byResult, this.summarizedResults[byResultKey][i]);
                // If it was a Simple Named Expression then the new column will come back in array itself
                // Other it will be tuple returned by Function Call Expression
                //debug_log("Aggregate result is " + JSON.stringify(aggregateResult));
                if (Array.isArray(aggregateResult)) {
                    this.summarizedResults[byResultKey][i][aggregateResult[0]] = aggregateResult[1];
                }
                else {
                    this.summarizedResults[byResultKey][i] = { ... this.summarizedResults[byResultKey][i],  ... aggregateResult }
                }
            }
        }
        
        // Construct back final summarized result to be sent back in result table
        // This is a freshly constructed object each time of processing (for now)
        // Send only the keys which have changed
        var resultTable = [];
        if (options["emit_interval"] == null || options["emit_interval"] == 0) {
            for (const key of changedKeys.values()) {
                var value = this.summarizedResults[key];
                // Merge all aggregates into one dictionary
                // TBD: There could be column name clashes
                var mergedRow = Object.assign({}, ... value);
                // For downstream operators to determine where the add or update during incremental processing
                mergedRow["__key__"] = key;
                resultTable.push(mergedRow);
            }
        }
        else {
            for (const key of Object.keys(this.summarizedResults)) {
                // Emity a key only if it not been emitted for more than "emit_interval" and it 
                // has been updated since last emit
                if (((Date.now() - this.emitHistory[key].lastEmitTime) >= options["emit_interval"]) &&
                         (this.emitHistory[key].lastEmitTime <= this.emitHistory[key].lastUpdateTime)) {
                    var value = this.summarizedResults[key];
                    // Merge all aggregates into one dictionary
                    // TBD: There could be column name clashes
                    var mergedRow = Object.assign({}, ... value);
                    // For downstream operators to determine where the add or update during incremental processing
                    mergedRow["__key__"] = key;
                    resultTable.push(mergedRow);
                                      
                    this.emitHistory[key].lastEmitTime = Date.now();
                }

                // TBD:: make expiry interval as parameter
                if ((Date.now()-this.emitHistory[key].lastUpdateTime) >= 5*options["emit_interval"]) {
                    delete this.summarizedResults[key];
                }
            }

            // Expire keys which have not been updated since expiry_interval (expiry interval should be set much higher than emit interval)
            // TBD
        }

        // Add data type information at the beginning
        resultTable.unshift(this.resultTypeMap);

        //debug_log("Final summarized results are : " + JSON.stringify(resultTable));
        this.end();
        return resultTable;
    }

    flush(context, result) {
        var resultTable = [];
        for (const key of Object.keys(this.summarizedResults)) {
            // Emit all keys which are waiting for emit_interval to be over
            if ((this.emitHistory[key] == null) || options["emit_interval"] == null || (Date.now() - this.emitHistory[key].lastEmitTime) < options["emit_interval"]) {
                var value = this.summarizedResults[key];
                // Merge all aggregates into one dictionary
                // TBD: There could be column name clashes
                var mergedRow = Object.assign({}, ... value);
                // For downstream operators to determine where the add or update during incremental processing
                mergedRow["__key__"] = key;
                resultTable.push(mergedRow);

                if (this.emitHistory[key] == null)
                    this.emitHistory[key] = {};

                this.emitHistory[key].lastEmitTime = Date.now();
                this.emitHistory[key].emitted = true;
            }
        }
        resultTable.unshift(this.resultTypeMap);
        return resultTable;        
    }
}

class NameReference  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType, isTable) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType, isTable);
    }
    
    evaluate(context, result) {
        //debug_log("In NameReference");
        // In 'null' result case just return null (this will happen in loadState call)
        if (result == null)
            return [null, null];

        if (result != null & result[this.simpleName] != null) {
            var value = result[this.simpleName];
            if (this.resultType == "long" || this.resultType == "int")
                value = parseInt(value);
            result = [this.simpleName, value];
        }
        else  if (context[this.simpleName] != null) {
            if (context["__type__"] == "table") {
                // In table context return the set of rows as is
                // Add data type only in case of table context
                var result = [ ...context[this.simpleName] ];
                result.unshift(this.resultTypeMap);
                //debug_log("Returning " + JSON.stringify(result));
                return result;
            }
            else {
                // Scalar context. Return a tuple of the column Name and value. 
                // Column name is needed by upstream evaluators like summarize by clause
                var value = context[this.simpleName];
                if (this.resultType == "long" || this.resultType == "int")
                    value = parseInt(value);
                result = [this.simpleName, value];            }
        }
        else if (context["__type__"] == "table") {
            // In table context return empty result
            return [this.resultTypeMap];
        }
        else {
            // Scalar context
            //debug_log("NameReference not found : " + this.simpleName + " context is " + JSON.stringify(context));
            result = [null, null];
        }

        //debug_log("Returning NameReference " + JSON.stringify(result));
        return result;
    }

    getUsedTables(usedTables) {
        if (this.isTable)
            usedTables.push([this.simpleName, null])

        return usedTables;
    }
}

class RealLiteralExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var real = parseFloat(this.text);
        //debug_log("Returning RealLiteralExpression " + JSON.stringify(real));
        return [null, real];
    }
}

class DynamicExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var begin = Date.now();
        var longVal = this.literalValue;
        ////debug_log("Returning DynamicExpression " + JSON.stringify(real));
        // dynamic([0, 1])
        // First element is 'dynamic' keyword
        // Second element is '('
        // Third element is the actual value which will become of dynamic type
        // Fourth element is ')'

        // The evaluate the third element and return that "as is" as the value of dynamic expression
        var result = this.children[2].evaluate(context, result);
        debug_log(this.kind + " took " + (Date.now() - begin));
        return [null, result];
    }
}
class LongLiteralExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var longVal = this.literalValue;
        ////debug_log("Returning LongLiteralExpression " + JSON.stringify(real));
        return [null, longVal];
    }
}

class StringLiteralExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        ////debug_log("Returning LongLiteralExpression " + JSON.stringify(real));
        return [null, this.literalValue];
    }
}

class TimespanLiteralExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Ticks unit is 1000x of milliseconds - which is ??
        //debug_log("Literal value " + JSON.stringify(this.literalValue));
        var timespanTicks = this.literalValue.ticks;
        //debug_log("Returning TimespanLiteralExpression " + JSON.stringify(timespanTicks/10000));
        return [null, timespanTicks/10000];
    }
}

class SimpleNamedExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        //debug_log("Command SimpleNamedExpression: " + this.text);
        // c = a + b
        // Expect three children: LHS assignment, "=" token, RHS expression
        var lhsVar = this.children[0].simpleName;
        //debug_log("Evaluating " + this.children[2].kind + " lhs " + lhsVar);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("RHS value is " + JSON.stringify(rhs));
        // Add/Update this variable to the result
        //result[lhsVar] = rhs[1];
        //debug_log("Returning SimpleNamedExpression " + JSON.stringify(result));
        var temp = {};
        temp[lhsVar] = rhs[1];
        return temp;
    }

    getResultTypeWithColumnName() {
        var r = {};
        r[this.children[0].simpleName] = this.resultType;
        return r;
    }
}

class ElementExpression  extends this.generic {
    totalTime = 0;
    f;
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
        this.f = new Function('context, result', 'try { return result.' + this.text + '; } catch (err) { try { return context.' + this.text + '; } catch (err) { return null; } }');
    }
    
    evaluate_old(context, result) {
        this.begin();
        //debug_log("Command ElementExpression: " + this.text);
        // Field[0][0]

        // Just take the literalValue and run it through Javascript evaluator using Function
        try {
            var temp = this.f(context, result);
            this.end();
            return [null, temp];
        }
        catch (err) {
            console.log("Error in Element expression " + this.literalValue);
            return [null, null];
        }
    
    }

    evaluate(context, result) {
        this.begin();

        // Evaluate each child in sequence
        // Send result of one to next
        for (var i=0; i < this.children.length; ++i) {
            result = this.children[i].evaluate(context, result);
        }

        this.end();
        return result;
    
    }
    getResultTypeWithColumnName() {
        var r = {};
        r[this.children[0].simpleName] = this.resultType;
        return r;
    }
}

class BracketedExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        this.begin();

        // [0]
        // First child is '(' token
        // Second child is the index
        // Third child is ')' token

        if (result[1] == null)
            return [null, null];

        // Evaluate the index
        var indexValue = this.children[1].evaluate(context, result);
        // Apply the index on incoming result
        var result = result[1][indexValue[1]];
        this.end();
        return [null, result];
    }
}

class ExpressionList  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        //debug_log("Command: ExpressionList " + this.text );
        // (StartTime % 1d , 1h)
        // First child is '(' token
        // Last child is ')' token
        // In between is a List which has the expressions to be evaluated separate by ',' tokens
        // Evaluate each one return the values in an array
        var values = [];
        var i;
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            //debug_log("Expression kind is " + expression.kind);
            if (expression.kind == 'CommaToken') {
                continue;
            }
            values.push(expression.evaluate(context, result));
            //debug_log("Values in expression are : " + JSON.stringify(values));
        }
        return values;
    }
}

class JsonArrayExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        //debug_log("Command: JsonArrayExpression " + this.text );
        // [0, 1]
        // First child is '[' token
        // Last child is ']' token
        // In between is a List which has the array elements to be evaluated separate by ',' tokens
        // Evaluate each one return the values in an array
        var values = [];
        var i;
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            //debug_log("Expression kind is " + expression.kind);
            if (expression.kind == 'CommaToken') {
                continue;
            }
            values.push(expression.evaluate(context, result));
            //debug_log("Values in expression are : " + JSON.stringify(values));
        }
        return values;
    }
}

class SummarizeByClause  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        //debug_log("Command: SummarizeByClause " + this.text );
        // by price_range=bin(price, 10.0)
        // First child is 'by' token
        // Followed by one or more expressions
        // Evaluate each one return the values in an array
        var values = [];
        var i;
        var columnCount=1;
        var byResult = {};
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            //debug_log("Expression kind is " + expression.kind);
            if (expression.kind == 'CommaToken') {
                continue;
            }
            
            var expressionResult = expression.evaluate(context, {});
            if (Array.isArray(expressionResult)) {
                if (expressionResult[0])
                    byResult[expressionResult[0]] = expressionResult[1];
                else
                    byResult['Column' + columnCount] = expressionResult[1];
            }else {
                byResult = { ... expressionResult,  ... byResult };
            }
            /*
            if (expressionResult[0] != null) {
                byResult[expressionResult[0]] = expressionResult[1];
            }
            else {
                byResult['Column' + columnCount] = expressionResult[1];
            }
            */
            ++columnCount;
        }
        //debug_log("Returning SummarizeByClause " + JSON.stringify(result));
        return byResult;
    }
}

class FunctionCallExpression  extends this.generic {
    evaluator = null;
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    init() {
        // Clear out the function cache
        if (this.evaluator != null) {
            this.evaluator.init();
        }
    }

    getState(state) {
        if (this.evaluator == null) {
            // evaluator may be null if the first query itself is loaded from cache
            var funcName = this.children[0].simpleName;
            var funcCall = funcMap[funcName];
            if (funcCall != null) {
                this.evaluator = new funcCall();
            }
        }
        state['text'] = this.text;
        state['state'] = this.evaluator.getState({});
        return state;
    }

    loadState(state) {
        if (this.evaluator == null) {
            // evaluator may be null if the first query itself is loaded from cache
            var funcName = this.children[0].simpleName;
            var funcCall = funcMap[funcName];
            if (funcCall != null) {
                this.evaluator = new funcCall();
            }
        }

        this.evaluator.loadState(state['state']);
    }
    
    evaluate(context, result) {
        this.begin();
        // e.g. floor(StartTime % 1d , 1h)
        // First child is the function token
        var funcName = this.children[0].simpleName;
        //debug_log("Function name is " + funcName);
        // Second child is a expression list. The expression list will result in one or more values
        // which will be parameters for the function to be evaluated.

        var params = this.children[1].evaluate(context, result);
        var funcCall = funcMap[funcName];
        if (funcCall != null) {
            if (this.evaluator == null)
                this.evaluator = new funcCall();
            
            var result = this.evaluator.evaluate(context, result, params);
            this.end();
            return [funcName + "_", result];                
        }
        else {
            //debug_log("Unsupported function " + funcName);
            return [null, null]; // Unsupported function
        }
    }
}

class ContainsExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children: expression to check in, "contains" token, subsequence string to look for
        var mainString = this.children[0].evaluate(context, result);
        var subsequence = this.children[2].evaluate(context, result);
        if (mainString[1] == null || subsequence[1] == null)
            return [null, false];
        //debug_log("Returning ModuloExpression " + JSON.stringify(lhs[1]%rhs[1]));
        return [null, mainString[1].toLowerCase().search(subsequence[1].toLowerCase()) == -1 ? false : true];
    }
}
class NotContainsExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children: expression to check in, "contains" token, subsequence string to look for
        var mainString = this.children[0].evaluate(context, result);
        var subsequence = this.children[2].evaluate(context, result);
        if (mainString[1] == null || subsequence[1] == null)
            return [null, false];
        //debug_log("Returning ModuloExpression " + JSON.stringify(lhs[1]%rhs[1]));
        return [null, mainString[1].toLowerCase().search(subsequence[1].toLowerCase()) == -1 ? true : false];
    }
}

class DivideExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, "/ token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("Returning DivideExpression " + JSON.stringify(lhs[1]%rhs[1]));
        return [null, lhs[1] / rhs[1]];
    }
}

class ModuloExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, "%" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("Returning ModuloExpression " + JSON.stringify(lhs[1]%rhs[1]));
        return [null, lhs[1] % rhs[1]];
    }
}
class AddExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, "+" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("Returning AddExpression" + JSON.stringify(lhs[1]+rhs[1]));
        return [null, lhs[1] + rhs[1]];
    }
}

class GreaterThanExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, ">" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("Returning GreaterThanExpression " + JSON.stringify(lhs[1]>rhs[1]));
        return [null, lhs[1] > rhs[1]];
    }
}

class EqualExpression  extends this.generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, ">" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        //debug_log("Returning EqualExpression " + JSON.stringify(lhs[1] == rhs[1]));
        return [null, lhs[1] == rhs[1]];
    }
}

class Strcat extends this.generic {
    evaluate(context, result, params) {
        var concatResult="";
        for (var i=0; i < params.length; ++i) {
            if (params[i] != null && params[i][1] != null)
                concatResult += params[i][1];
        }
        return concatResult;
    }
}

class Strlen extends this.generic {
    evaluate(context, result, params) {
        if (!params[0][1])
            return 0;
        // Convert to string by prefixing empty string
        return (""+params[0][1]).length;
    }
}

class ToLong extends this.generic {
    evaluate(context, result, params) {
        if (!params[0][1])
            return null;
        return parseInt(params[0][1]);
    }
}

class ToReal extends this.generic {
    evaluate(context, result, params) {
        if (!params[0][1])
            return null;
        return parseFloat(params[0][1]);
    }
}

class ToString extends this.generic {
    evaluate(context, result, params) {
        if (!params[0][1])
            return null;
        if (typeof params[0][1] == 'object' &&
            params[0][1].constructor.name == 'Date') {
                return params[0][1].toISOString();
        }
        else {
            return params[0][1].toString();
        }
    }
}

class Floor extends this.generic {
    evaluate(context, result, params) {
        var value = params[0][1];
        var roundTo = params[1][1];
        return Math.floor(value/roundTo)*roundTo;
    }
}

class ExtractAll extends this.generic {
    evaluate(context, result, params) {
        var begin = Date.now();
        var regex = new RegExp(params[0][1], 'g');
        var match;
        var allMatches = [];
        if (params.length == 2) {
            while (match = regex.exec(params[1][1])) {
                for (var i=1; i < match.length; ++i)
                    allMatches.push(match[i]);
            }
        }
        else {
            while (match = regex.exec(params[2][1])) {
                var captureGroups = params[1][1];
                var matches = [];
                for (var i=0; i < captureGroups.length; ++i) {
                    matches.push(match[captureGroups[i][1]]);
                }
                allMatches.push(matches);
            }
        }
        debug_log("extract_all " + " took " + (Date.now() - begin));

        if (allMatches.length > 0)
            return allMatches;
        else
            return null;
    }
}

class ExtractJSON extends this.generic {
    evaluate(context, result, params) {
        // TBD:: Support type conversion
        var f = new Function('jsonText', 'var $ = JSON.parse(jsonText); return ' + params[0][1] + ';');
        try {
            return f(params[1][1]);
        }
        catch (err) {
            console.log("Error in JSON extraction " + params[0][1]);
        }
    }
}

class Replace extends this.generic {
    evaluate(context, result, params) {
        var regex = new RegExp(params[0][1], 'g');
        if (params[2][1] != null) {
            return params[2][1].replace(regex, params[1][1]);
        }

        return null;
    }
}

class ToDateTime extends this.generic {
    evaluate(context, result, params) {
        if (params[0][1] != null) {
            return new Date(Date.parse(params[0][1]));
        }

        return null;
    }
}

class IsNotNull extends this.generic {
    evaluate(context, result, params) {
        return (params[0][1] != null)
    }
}

class IsNull extends this.generic {
    evaluate(context, result, params) {
        return (params[0][1] == null)
    }
}

class Count extends this.generic {
    state = {};

    init() {
        this.state = {};
    }
    
    getState(state) {
        state['state'] = this.state;
        state['text'] = this.text;
        return state;
    }

    loadState(newstate) {
        for (var key of Object.keys(newstate['state'])) {
            if (this.state[key] != null) {
                this.state[key] += newstate['state'][key];
            }
            else {
                this.state[key] = newstate['state'][key];
            }
        }
    }

    evaluate (context, result, params) {
        if (result == null) {
            return this.state[context["__key__"]];
        }
        else if (!this.state[context["__key__"]])
            this.state[context["__key__"]] = 0;

        ++this.state[context["__key__"]];
        return this.state[context["__key__"]];
    }
}

class DCount extends this.generic {
    state = {};

    init() {
        this.state = {};
    }
    
    getState(state) {
        state['state'] = [];
        for (var key of this.state) {
            state['state'][key] = [];
            for (var el of Object.keys(this.state[key])) {
                state['state'][key].push(el);
            }
        }
        state['text'] = this.text;
        return state;
    }

    loadState(newstate) {
        for (var key of Object.keys(newstate['state'])) {
            if (this.state[key] != null) {
                newstate['state'][key].forEach(item => this.state[key].add(item));
            }
            else {
                this.state[key] = new Set(newstate['state'][key]);
            }
        }
    }

    evaluate(context, result, params) {
        if (result == null) {
            return this.state[context["__key__"]].size;
        }

        if (!params[0][1]) {
            if (this.state[context["__key__"]])
                return this.state[context["__key__"]].size;
            else
                return 0;
        }

        if (this.state[context["__key__"]]) {
            this.state[context["__key__"]] =  new Set();
        }

        this.state[context["__key__"]].add(params[0][1]);
        return this.state[context["__key__"]].size;
    }
}

class Sum extends this.generic {
    state = {};
    
    init() {
        this.state = {};
    }
    
    getState(state) {
        state['state'] = this.state;
        state['text'] = this.text;
        return state;
    }

    loadState(newstate) {
        for (var key of Object.keys(newstate['state'])) {
            if (this.state[key] != null) {
                this.state[key] += newstate['state'][key];
            }
            else {
                this.state[key] = newstate['state'][key];
            }
        }
    }

    evaluate(context, result, params) {
        if (result == null) {
            return this.state[context["__key__"]];
        }

        if (!params[0][1]) {
            if (this.state[context["__key__"]])
                return this.state[context["__key__"]];
            else
                return 0;
        }

        if (!this.state[context["__key__"]]) {
            this.state[context["__key__"]] = 0.0;
        }
        this.state[context["__key__"]] = this.state[context["__key__"]]+ params[0][1];
        return this.state[context["__key__"]];
        }
}

class Avg extends this.generic {
    state = {};

    init() {
        this.state = {};
    }
    
    evaluate(context, result, params) {
        if (!params[0][1]) {
            if (this.state[context["__key__"]])
                return this.state[context["__key__"]][0]/this.state[context["__key__"]][1];
            else
                return null;
        }

        if (!this.state[context["__key__"]]) {
            this.state[context["__key__"]] = [0.0, 0.0];
        }
        this.state[context["__key__"]][0] = this.state[context["__key__"]][0] + params[0][1];
        ++this.state[context["__key__"]][1];
        return this.state[context["__key__"]][0]/this.state[context["__key__"]][1];
    }
}

class MakeBag extends this.generic {
    state = {};

    init() {
        this.state = {};
    }
    
    evaluate(context, result, params) {
        if (!params[0][1])
        return;

        if (!this.state[context["__key__"]]) {
            this.state[context["__key__"]] = {};
        }
        if (params[0][1].constructor == Object) {
            this.state[context["__key__"]] = { ... params[0][1], ... this.state[context["__key__"]] };
        }
        return this.state[context["__key__"]];
    }
}

class Pack extends this.generic {
    evaluate(context, result, params) {
        var packResult = {};
        for (var i=0; i < params.length; i=i+2) {
            packResult[params[i][1]] = params[i+1][1];
        }
        return packResult;
    }
}

var funcMap = {
    // Scalar functions
    "floor" : Floor,
    "bin" : Floor,
    "tostring" : ToString,
    "tolong" : ToLong,
    "toreal" : ToReal,
    "todouble" : ToReal,
    "extract_all" : ExtractAll,
    "extractjson" : ExtractJSON,
    "isnotnull" : IsNotNull,
    "isnull" : IsNull,
    "pack": Pack,
    "replace" : Replace,
    "todatetime" : ToDateTime,
    "strcat" : Strcat,
    "strlen" : Strlen,

    // Aggregation functions
    "count" : Count,
    "dcount" : DCount,
    "sum" : Sum,
    "avg" : Avg,
    "make_bag" : MakeBag
}

var evaluatePluginMap = {
    "searchindex" : SearchIndexPlugin,
    "bag_unpack" :BagUnpackPlugin
}

var classMap = {
    "NameReference": NameReference,

    // Operators
    "ProjectOperator": ProjectOperator,
    "ExtendOperator": ExtendOperator,
    "FilterOperator": FilterOperator,
    "SummarizeOperator": SummarizeOperator,
    "CountOperator": CountOperator,
    "TakeOperator": TakeOperator,
    "EvaluateOperator": EvaluateOperator,
    "SearchOperator": SearchOperator,
    "MvApplyOperator": MvApplyOperator,
    "ProjectRenameOperator" : ProjectRenameOperator,

    // Expressions
    "ExpressionList" : ExpressionList,

    "SummarizeByClause": SummarizeByClause,

    "AddExpression": AddExpression,
    "SimpleNamedExpression": SimpleNamedExpression,
    "ElementExpression": ElementExpression,
    "ContainsExpression": ContainsExpression,
    "NotContainsExpression": NotContainsExpression,
    "ModuloExpression": ModuloExpression,
    "DivideExpression": DivideExpression,
    "GreaterThanExpression": GreaterThanExpression,
    "EqualExpression": EqualExpression,
    "RealLiteralExpression": RealLiteralExpression,
    "LongLiteralExpression": LongLiteralExpression,
    "StringLiteralExpression": StringLiteralExpression,
    "FunctionCallExpression": FunctionCallExpression,
    "TimespanLiteralExpression": TimespanLiteralExpression,
    "JsonArrayExpression": JsonArrayExpression,
    "DynamicExpression": DynamicExpression,
    "BracketedExpression": BracketedExpression,

    // Control commands
    "ExportKeyword_CustomNode" : Export,

    // Statements
    "SetOptionStatement" : SetOptionStatement

}

// module.exports = Generic;
// module.exports = traverse;
