const http = require('http');
const WebSocketServer = require('websocket').server;
const SimpleNodeLogger = require('simple-node-logger');
const kustoExecutor = require('./kustoExecutor.js').KustoExecutor;
const queryConf = require('./conf.js').conf;

const opts = {
  logFilePath:'app.log',
  timestampFormat:'YYYY-MM-DD HH:mm:ss.SSS'
}

let openConnections = 0;

const log = SimpleNodeLogger.createSimpleLogger( opts );
log.setLevel('info');

const port = 5001;

const server = http.createServer((req, res) => {
  if (req.url == '/fetchschema') {
    const sch = getSchema();

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.write(JSON.stringify(sch));
    res.end();
  }
});
server.listen(port);

const wsServer = new WebSocketServer({
  httpServer: server,
  keepaliveInterval: 300000
});

//emitData();

wsServer.on('request', function(request) {
  const connection = request.accept(null, request.origin);

  openConnections++;
  log.info("Number of open requests on this node: " + openConnections)
  
  connection.on('message', function(message) {
    const reqData = JSON.parse(message.utf8Data);
    const pivot = reqData.pivot;
    const realTime = reqData.realTime;

    log.info("Running panel id: " + reqData.panelId + ", query: ", JSON.stringify(reqData.query) + 
    ", pivot: " + pivot + ", realTime: " + realTime );

    connection.handler = executeQuery(reqData.query, connection);
  })

  connection.on('close', function(reasonCode, description) {
    log.info('Client has disconnected. Reason: ' + reasonCode + ' Description: ' + description);
    
    if (connection.handler) {
      connection.handler.cancel();
      log.info('Canceled handler');
    }

    openConnections--;
    if (openConnections === 0) {
      console.log("No open request. Terminating the process")
      //throw "Exit"
    }
  });
});

function executeQuery(query, connection) {
  var ex = new kustoExecutor();

  ex.on("data", (result) => { 
    try {
      connection.sendUTF(JSON.stringify({active: 0, data: result}));
    }
    catch (e) {
      log.error("Error in sending data " + e);
    }
  });

  ex.on("end", () => { console.log("Completed"); });
  const fromTime = new Date(query.from);
  const toTime = new Date(query.to);
  return ex.execute(JSON.stringify(queryConf), query.kusto, "timestamp", fromTime.toISOString(), toTime.toISOString());
}

function getSchema() {
  return {
    Plugins: [],
    Databases: {
      kafka: {
        Name: 'kafka',
        Tables: {
          ContainerInsights: {
            Name: 'ContainerInsights',
            OrderedColumns: [
              { Name: 'timestamp', Type: 'System.String', CslType: 'string' },
              { Name: 'message', Type: 'System.String', CslType: 'string' },
            ],
          },
          realtime: {
            Name: 'realtime',
            OrderedColumns: [
              { Name: 'timestamp', Type: 'System.String', CslType: 'string' },
              { Name: 'message', Type: 'System.String', CslType: 'string' },
            ],
          },
        },
        MajorVersion: 7,
        MinorVersion: 5,
        Functions: {},
        DatabaseAccessMode: 'ReadWrite',
        ExternalTables: {},
        MaterializedViews: {},
      },
      solr: {
        Name: 'Solr',
        Tables: {
          MySolr: {
            Name: 'ContainerInsights',
            OrderedColumns: [
              { Name: 'timestamp', Type: 'System.String', CslType: 'string' },
              { Name: 'message', Type: 'System.String', CslType: 'string' },
            ],
          }
        },
        MajorVersion: 7,
        MinorVersion: 5,
        Functions: {},
        DatabaseAccessMode: 'ReadWrite',
        ExternalTables: {},
        MaterializedViews: {},
      },
    },
  }
}
