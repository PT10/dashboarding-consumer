const bridge = require('./kustolib/bridge.js/index.js');
//const bridge_console = require('./bridge.console.js');
//const bridge_meta = require('./bridge.meta.js');
const kusto = require('./kustolib/Kusto.Language.Bridge.js/index.js');
//const kusto_meta = require('./Kusto.Language.Bridge.meta.js');
//var util = require('util');

const csv = require('csv-parser');
const fs = require('fs')

var debug = false;

// UNCOMMENT this statement to run StormEvent queries
var database = new Kusto.Language.Symbols.DatabaseSymbol("db", [
    new Kusto.Language.Symbols.TableSymbol.$ctor3("StormEvents", [
        new Kusto.Language.Symbols.ColumnSymbol("StartTime", Kusto.Language.Symbols.ScalarTypes.DateTime)
    ])
  ]);
  var globals = Kusto.Language.GlobalState.Default.WithDatabase(database);
  var query = Kusto.Language.KustoCode.ParseAndAnalyze("StormEvents | extend hour = floor(StartTime % 1d , 1h) | summarize event_count1=count(), event_count2=count() by hour", globals);
  
function debug_log(msg) {
    if (debug) {
        console.log(msg);
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
        var r = {};
        var i;
        for (i=0; i < columns.length; ++i) {
            r[columns[i].Name] = datatypeToString(columns[i].Type)
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
        case Kusto.Language.Symbols.ScalarTypes.Timespan:
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

function traverse(node, level, currentTree) {
    var i;
    if (!node)
        return;
    kindInt = node.Kind;
    kind = string_of_enum(node.Kind);
    text = node.toString(Kusto.Language.Syntax.IncludeTrivia.Minimal);
    simpleName = node.SimpleName;
    literalValue = node.LiteralValue;
    isLiteral = node.IsLiteral;
    resultType = datatypeToString(node.ResultType);

    const evaluatorClass = classMap[kind];

    var evaluatorObject;

    if (evaluatorClass != null) {
        evaluatorObject = new evaluatorClass(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    else {
        evaluatorObject = new Generic(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    currentTree.children.push(evaluatorObject);

    for (i=0; i < node.ChildCount; ++i) {
        child = node.GetChild(i);
        traverse(child, level+1, evaluatorObject);
    }
}

class Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        this.kindInt = kindInt;
        this.kind = kind;
        this.text = text;
        this.simpleName = simpleName;
        this.literalValue = literalValue;
        this.isLiteral = isLiteral;
        this.resultType = resultType;
        this.children = [];
        this.resultTypeMap = this.resultType;
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

    createResultTypeMap() {
        // In generic case just call in each of the descendant nodes
        var i;
        for (i=0; i < this.children.length; ++i) {
            this.children[i].createResultTypeMap();
        }
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
}

class CountOperator extends Generic {
    uniqueSet = new Set();
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        //debug_log("Count operator " + JSON.stringify(context));
        var j;
        for (j=0; j < result.length; ++j) {
            debug_log("Count operator " + JSON.stringify(result[j]));
            this.uniqueSet.add(result[j]["__key__"]);
        }
        return [{ "count" : "long" }, { "__key__" : "count", "count" : this.uniqueSet.size}];
    }

    createResultTypeMap() {
        this.resultTypeMap = { "count" : "long" };
    }
}

class ProjectOperator extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        debug_log("Project operator context " + JSON.stringify(context));
        var j;
        for (j=1; j < result.length; ++j) {
            debug_log("Project operator result " + JSON.stringify(result[j]));
            // Expect two children: "project" keyword and list of project expressions
            var i;
            var projections = this.children[1];
            for (i=0; i < projections.children.length; ++i) {
                // Note the change in context below
                result[j] = projections.children[i].evaluate(result[j], result[j]);
            }
        }
        result[0] = { ... result[0], ... this.resultTypeMap };
        return result;
    }

    createResultTypeMap() {
        var projections = this.children[1];
        var i;
        for (i=0; i < projections.children.length; ++i) {
            // Note the change in context below
            this.resultTypeMap = { ...this.resultTypeMap, ...projections.children[i].getResultTypeWithColumnName() };
        }
    }
}

class ExtendOperator extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }

    evaluate(context, result) {
        var resultTable = [];
        var j;
        for (j=1; j < result.length; ++j) {
        // Expect two children: "extend" keyword and list of extend expressions
        var i;
            var extendss = this.children[1];
            for (i=0; i < extendss.children.length; ++i) {
                result[j] = extendss.children[i].evaluate(result[j], result[j]);
            }
        }
        result[0] = { ... result[0], ... this.resultTypeMap };
        return result;
    }

    createResultTypeMap() {
        var projections = this.children[1];
        var i;
        for (i=0; i < projections.children.length; ++i) {
            // Note the change in context below
            this.resultTypeMap = { ...this.resultTypeMap, ...projections.children[i].getResultTypeWithColumnName() };
        }
    }
}

class FilterOperator  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var resultTable = [];
        var j;
        for (j=1; j < result.length; ++j) {
            // Expect two children ideally: "where" keyword and predicate but I am seeing an empty list in between
            var predicate = this.children[2];
            var predicateResult = predicate.evaluate(result[j], result[j]);
            if (predicateResult[1]) {
                resultTable.push(result[j]);
            }
        }

        // Filter operator doesn't add or change data types
        resultTable.unshift(result[0]);
        return resultTable;
    }
}

class SummarizeOperator  extends Generic {
    summarizedResults = {};
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
    
    evaluate(context, result) {

        var j;
        for (j=1; j < result.length; ++j) {       
            // First child is 'summarize' token. Followed by empty list (not sure what it is)
            // Followed by List of aggregate expressions

            // Next is the 'by' clause

            // First the by clause will be processed
            // The 'by' clause can be thought of new temporary "extends" for the purpose of aggregation
            // So we will process each by clause like any other expression and let it add new columns when required
            // In the result 'by' clause evaluator will return the list of 'by' columns.
            var byClause = this.children[3];
            // Switch context to incoming result because now we are inside row now
            var byClauseResult = byClause.evaluate(result[j], {});
            debug_log("By Clause result is " + JSON.stringify(byClauseResult));

            // Merge incoming results with new/updated columns created by by clause
            var byResult = { ... result[j], ... byClauseResult};
            byResult["__type__"] = "table";

            var byResultKey = Object.values(byClauseResult).join();
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
                var aggregateResult = aggregate.evaluate(byResult, this.summarizedResults[byResultKey][i]);
                // If it was a Simple Named Expression then the new column will come back in array itself
                // Other it will be tuple returned by Function Call Expression
                debug_log("Aggregate result is " + JSON.stringify(aggregateResult));
                if (Array.isArray(aggregateResult)) {
                    this.summarizedResults[byResultKey][i][aggregateResult[0]] = aggregateResult[1];
                }
            }
        }
        
        // Construct back final summarized result to be sent back in result table
        // This is a freshly constructed object each time of processing (for now)
        var resultTable = [];
        for (const [key, value] of Object.entries(this.summarizedResults)) {
            // Merge all aggregates into one dictionary
            // TBD: There could be column name clashes
            var mergedRow = Object.assign({}, ... value);
            // For downstream operators to determine where the add or update during incremental processing
            mergedRow["__key__"] = key;
            resultTable.push(mergedRow);
        }

        // Add data type information at the beginning
        resultTable.unshift(this.resultTypeMap);

        debug_log("Final summarized results are : " + JSON.stringify(resultTable));
        return resultTable;
    }
    
    createResultTypeMap() {
        var projections = this.children[1];
        var i;

        // Result type map for summarize is combination of result type map of
        // 'by' clause columns and aggregate expressions
        this.resultTypeMap = this.children[3].getResultTypeWithColumnName();

        for (i=0; i < this.children[2].children.length; ++i) {
            var aggregate = this.children[2].children[i];
            if (aggregate.kind == 'CommaToken') {
                continue;
            }
            this.resultTypeMap = { ...this.resultTypeMap, ...aggregate.getResultTypeWithColumnName()}
        }
    }
}
class NameReference  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        debug_log("In NameReference");
        if (context[this.simpleName] != null) {
            if (context["__type__"] == "table") {
                // In table context return the set of rows as is
                // Add data type only in case of table context
                var result = [ ...context[this.simpleName] ];
                result.unshift(this.resultType);
                debug_log("Returning " + JSON.stringify(result));
                return result;
            }
            else {
                // Scalar context. Return a tuple of the column Name and value. 
                // Column name is needed by upstream evaluators like summarize by clause
                result = [this.simpleName, context[this.simpleName]];
            }
        }
        else {
            debug_log("NameReference not found : " + this.simpleName + " context is " + JSON.stringify(context));
            result = [null, null];
        }

        debug_log("Returning NameReference " + JSON.stringify(result));
        return result;
    }
}
class RealLiteralExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        var real = parseFloat(this.text);
        debug_log("Returning RealLiteralExpression " + JSON.stringify(real));
        return [null, real];
    }
}
class TimespanLiteralExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Ticks unit is 1000x of milliseconds - which is ??
        debug_log("Literal value " + JSON.stringify(this.literalValue));
        var timespanTicks = this.literalValue.ticks;
        debug_log("Returning TimespanLiteralExpression " + JSON.stringify(timespanTicks/10000));
        return [null, timespanTicks/10000];
    }
}

class SimpleNamedExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        debug_log("Command SimpleNamedExpression: " + this.text);
        // c = a + b
        // Expect three children: LHS assignment, "=" token, RHS expression
        var lhsVar = this.children[0].simpleName;
        debug_log("Evaluating " + this.children[2].kind + " lhs " + lhsVar);
        var rhs = this.children[2].evaluate(context, result);
        debug_log("RHS value is " + JSON.stringify(rhs));
        // Add/Update this variable to the result
        result[lhsVar] = rhs[1];
        debug_log("Returning SimpleNamedExpression " + JSON.stringify(result));
        return result;
    }

    getResultTypeWithColumnName() {
        var r = {};
        r[this.children[0].simpleName] = this.resultType;
        return r;
    }
}

class ExpressionList  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        debug_log("Command: ExpressionList " + this.text );
        // (StartTime % 1d , 1h)
        // First child is '(' token
        // Last child is ')' token
        // In between is a List which has the expressions to be evaluated separate by ',' tokens
        // Evaluate each one return the values in an array
        var values = [];
        var i;
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            debug_log("Expression kind is " + expression.kind);
            if (expression.kind == 'CommaToken') {
                continue;
            }
            values.push(expression.evaluate(context, result));
            debug_log("Values in expression are : " + JSON.stringify(values));
        }
        return values;
    }
}

class SummarizeByClause  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        debug_log("Command: SummarizeByClause " + this.text );
        // by price_range=bin(price, 10.0)
        // First child is 'by' token
        // Followed by one or more expressions
        // Evaluate each one return the values in an array
        var values = [];
        var i;
        var columnCount=0;
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            debug_log("Expression kind is " + expression.kind);
            if (expression.kind == 'CommaToken') {
                continue;
            }
            
            var expressionResult = expression.evaluate(context, {});
            if (expressionResult[0] != null) {
                result[expressionResult[0]] = expressionResult[1];
            }
            else {
                result['Column' + columnCount] = expressionResult[1]
            }
            ++columnCount;
        }
        debug_log("Returning SummarizeByClause " + JSON.stringify(result));
        return result;
    }

    getResultTypeWithColumnName() {
        var projections = this.children[1];
        var i;
        var resultTypeMap = {}
        for (i=0; i < this.children[1].children.length; ++i) {
            var expression = this.children[1].children[i];
            resultTypeMap = { ...resultTypeMap, ...expression.getResultTypeWithColumnName() };
        }
        return resultTypeMap;
    }
}
class FunctionCallExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // e.g. floor(StartTime % 1d , 1h)
        // First child is the function token
        var funcName = this.children[0].simpleName;
        debug_log("Function name is " + funcName);
        // Second child is a expression list. The expression list will result in one or more values
        // which will be parameters for the function to be evaluated.

        var params = this.children[1].evaluate(context, result);
        var funcCall = funcMap[funcName];
        if (funcCall != null) {
            debug_log("Calling " + funcName + " with parameters " + JSON.stringify(params) + " and context " + JSON.stringify(context) +
                        " and result " + JSON.stringify(result));
            if (context["__type__"] == "table")
                return [funcName, funcCall(context, result, params)];
            else
                return [funcName, funcCall(params)];
        }
        else {
            debug_log("Unsupported function " + funcName);
            return [null, null]; // Unsupported function
        }
    }
}

class ModuloExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, "%" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        debug_log("Returning ModuloExpression " + JSON.stringify(lhs[1]%rhs[1]));
        return [null, lhs[1] % rhs[1]];
    }
}
class AddExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, "+" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        debug_log("Returning AddExpression" + JSON.stringify(lhs[1]+rhs[1]));
        return [null, lhs[1] + rhs[1]];
    }
}

class GreaterThanExpression  extends Generic {
    constructor(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType) {
        super(kindInt, kind, text, simpleName, literalValue, isLiteral, resultType);
    }
    
    evaluate(context, result) {
        // Expect three children LHS, ">" token, RHS
        var lhs = this.children[0].evaluate(context, result);
        var rhs = this.children[2].evaluate(context, result);
        debug_log("Returning GreaterThanExpression " + JSON.stringify(lhs[1]>rhs[1]));
        return [null, lhs[1] > rhs[1]];
    }
}

function kFloor(params) {
    value = params[0][1];
    roundTo = params[1][1];
    return Math.floor(value/roundTo)*roundTo;
}

function kCount(context, result, params) {
    if (!result["__value__"]) {
        result["__value__"] = 0;
    }
    ++result["__value__"];
    return result["__value__"];
}

var funcMap = {
    // Scalar functions
    "floor" : kFloor,

    // Aggregation functions
    "count" : kCount
}

var classMap = {
    "NameReference": NameReference,

    // Operators
    "ProjectOperator": ProjectOperator,
    "ExtendOperator": ExtendOperator,
    "FilterOperator": FilterOperator,
    "SummarizeOperator": SummarizeOperator,
    "CountOperator": CountOperator,

    // Expressions
    "ExpressionList" : ExpressionList,

    "SummarizeByClause": SummarizeByClause,

    "AddExpression": AddExpression,
    "SimpleNamedExpression": SimpleNamedExpression,
    "ModuloExpression": ModuloExpression,
    "GreaterThanExpression": GreaterThanExpression,
    "RealLiteralExpression": RealLiteralExpression,
    "FunctionCallExpression": FunctionCallExpression,
    "TimespanLiteralExpression": TimespanLiteralExpression

}


var evaluatorTree = new Generic(-1, "", "", "", false)

  traverse(query.Syntax, 0, evaluatorTree);

  var data =  { "__type__": "table" , "StormEvents" : []};

  fs.createReadStream('StormEventsSmall.csv')
  .pipe(csv(["StartTime","EndTime","EpisodeId","EventId","State","EventType","InjuriesDirect","InjuriesIndirect","DeathsDirect","DeathsIndirect","DamageProperty","DamageCrops","Source","BeginLocation","EndLocation","BeginLat","BeginLon","EndLat","EndLon","EpisodeNarrative","EventNarrative","StormSummary"]))
  .on('data', (row) => {  
    if (!Date.parse(row['StartTime']))
        return;

    //debug_log("INPUT: " + JSON.stringify(row));
    // 2007-01-01T00:00:00Z
    //debug_log(Date.parse(row['StartTime']));
    row['StartTime'] = Date.parse(row['StartTime']);
    row['EndTime'] = Date.parse(row['EndTime']);
    data["StormEvents"].push(row);
    if (data['StormEvents'].length == 1000) {
        var output = JSON.stringify(evaluatorTree.evaluate(data, {}));
        //console.log("OUTPUT: " + output);
        data['StormEvents'] = [];
    }
    //debug_log("OUTPUT: " + JSON.stringify(evaluatorTree.evaluate({"__type__" : "table", "StormEvents" : row}, {})));
  })
  .on('end', () => {
    console.log("OUTPUT: " + JSON.stringify(evaluatorTree.evaluate(data, {})));
    //debug_log('CSV file successfully processed');
    //resultMap[panelId]['data'] = JSON.stringify(evaluatorTree.evaluate(data, {}));
    //return JSON.stringify(evaluatorTree.evaluate(data, {}));
  });


