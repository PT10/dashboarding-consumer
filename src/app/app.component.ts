import { DataService } from './data.service';
import { AfterViewInit, Component } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Subject } from 'rxjs';
import { MessageService } from 'primeng/components/common/messageservice';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
  providers: [MessageService]
})
export class AppComponent implements AfterViewInit {
  title = 'dashConsumeNew';

  private eventList= {};
  private wsUrl;

  private liveConections = {}

  constructor(public myDashServ: DataService, public messageService: MessageService) {}

  ngAfterViewInit() {
    this.myDashServ.getDashboardData().subscribe((data: any) => {
      this.myDashServ.getWebsocketUrl().subscribe((urlData: any) => {
        this.wsUrl = urlData['websocket_url'];
        this.myDashServ.dashboards = data;
        this.addSubscriptios();
      })
      
    }, error => {
      alert("Error");
    })

  }

  private addSubscriptios() {
    this.myDashServ.dashboards[0].data.forEach(panel => {
      if (panel.chartOptions.subscriptions && panel.chartOptions.subscriptions.length > 0) {
        panel.chartOptions.subscriptions.forEach((s: string) => {
          let subject = new Subject<any>();
          const varName = s.slice(0, s.indexOf('('))
          const paramNames = s.slice(s.indexOf('(') + 1, s.indexOf(')'));

          if (!this.eventList[varName]) {
            this.eventList[varName] = [];
          }
          this.eventList[varName].push({
            panelId: panel.panelId,
            subject: subject,
            observable: subject.asObservable().subscribe(d => {
              let changeDetected = false;

              if ( paramNames) {
                const params = paramNames.split(',');
                if (panel.chartOptions[params[0].trim()] !== d.data.data) {
                  panel.chartOptions[params[0].trim()] = d.data.data;
                  changeDetected = true;
                }

                if (d.data.timeWindow && 
                  params[1] && params[1].trim() === 'timeWindow' &&
                  JSON.stringify(panel.chartOptions['timeWindow']) !== JSON.stringify(d.data.timeWindow)) {
                    panel.chartOptions['timeWindow'] = d.data.timeWindow;
                    changeDetected = true;
                }
              }

              // if (!changeDetected) {
              //   return; // No change in values
              // }

              // Clear previous data
              panel.chartOptions.dataset.source = []

              // If there is a live conection close it
              if (this.liveConections[panel.panelId]) {
                try {
                  this.liveConections[panel.panelId].complete();
                  delete this.liveConections[panel.panelId];
                } catch {}
              }

              
              
              if (!panel.chartOptions.searchQuery || !panel.chartOptions.timeWindow || panel.chartOptions.timeWindow.length !== 2) {
                return;
              }

              const q: string = (panel.chartOptions.searchQueryPrefix || '') + panel.chartOptions.searchQuery + (panel.chartOptions.searchQueryExtention || '');
              const startTime = panel.chartOptions.timeWindow[0];
              const endTime: Date = new Date(panel.chartOptions.timeWindow[1])

              const additionalParams = panel.chartOptions.additionalParams || Math.random().toString(36).substring(7);

              const ws = this.getSocketConn();
              this.liveConections[panel.panelId] = ws;
              ws.subscribe(
                msg => {
                  if (msg && msg.data) {
                    msg.data[0]['__header__'].map(c => {
                      c["field"] = c.name
                      c["header"] = c.name
                      c["sortable"]  = true
                      c["searchable"] = false
                      c["resizable"] = true
                      c["width"] = ['msg', 'message'].includes(c.name) ? '50%' : '10%'
                    });

                    panel.chartOptions.chartConfig.columns = msg.data[0]['__header__'];

                    if (msg.data.length > 1) {
                      // msg.data.slice(1).forEach(d => {
                      //   panel.chartOptions.dataset.source.push(d); //.filter(d => d[panel.chartOptions.chartConfig.columns[0]['field']] != null);
                      // })
                      panel.chartOptions.dataset.source = panel.chartOptions.dataset.source.concat(msg.data.slice(1))
                      panel.chartOptions.loadStatus = "Completed";
                    }

                    if (!panel.chartOptions.chartConfig.columns || panel.chartOptions.chartConfig.columns.length === 0) {
                      if (panel.chartOptions.dataset.source && panel.chartOptions.dataset.source.length > 0) {
                        const keys = Object.keys(panel.chartOptions.dataset.source[0])
                        panel.chartOptions.chartConfig.columns = [];
                        keys.forEach(k => 
                          panel.chartOptions.chartConfig.columns.push({
                              field: k,
                              header: k,
                              sortable: true,
                              searchable: false,
                              resizable: true,
                              width: ['msg', 'message'].includes(k) ? '75%' : '25%'
                          }))
                      }
                    }
                  }
                }, error => {
                  this.messageService.add({key: 'tc', severity:'error', summary: 'Error connecting server', detail:'Websocket server is not running'});
                }, () => {

                }
              );

              //this.cnt += 3
              const factor = q.startsWith('.export') ? 0.5 : 1;
              setTimeout(() => {
                //panel.chartOptions.dataset.source = []
                panel.chartOptions.loadStatus = "Loading";

                ws.next({query: q, from: startTime, to: endTime, pivot: false, 
                  realTime: panel.chartOptions.realtime, additionalParams: additionalParams})
              }, factor * 1000);

              if (panel.chartOptions.chartConfig.onQueryExecuted) {
                const event: string = panel.chartOptions.chartConfig.onQueryExecuted;

                this.processEvent({
                  data: {data: additionalParams, timeWindow: panel.chartOptions.timeWindow}, 
                  variable: event
                });
              }
            })
          })
        })
      }
    });
  }

  processEvent(_data) {
    const panel = _data.panel;
    const variable = _data.variable;
    const data = _data.data;

    if (this.eventList[variable]) {
      this.eventList[variable].forEach(ev => {
        ev.subject.next(_data);
      });
    }
  }

  processCompletionEvent(_data) {

  }

  myCallback() {
    alert('Updated');
  }

  generateRandomMessage() {
    return {message: "This is a random message with id: " + Math.random(), host: 'host1', timestamp: new Date()}
  }

  private getSocketConn(): WebSocketSubject<any> {
    return webSocket({
      url: this.wsUrl,
      openObserver: {
        next: () => {
            console.log('connection ok');
        }
      },
      closeObserver: {
        next: () => {
            console.log('disconnect ok');
        }
      }
    })
  }
}
