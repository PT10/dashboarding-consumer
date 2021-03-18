import { DataService } from './data.service';
import { AfterViewInit, Component } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Subject } from 'rxjs';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements AfterViewInit {
  title = 'dashConsumeNew';
  startTime: any;

  sub1;
  sub2;

  private eventList= {};

  constructor(public myDashServ: DataService) {}

  ngAfterViewInit() {
    this.myDashServ.getDashboardData().subscribe((data: any) => {
      this.myDashServ.dashboards = data;
      //this.updateData();
      this.addSubscriptios();
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
          const paramName = s.slice(s.indexOf('(') + 1, s.indexOf(')'));

          if (!this.eventList[varName]) {
            this.eventList[varName] = [];
          }
          this.eventList[varName].push({
            panelId: panel.panelId,
            subject: subject,
            observable: subject.asObservable().subscribe(d => {
              panel.chartOptions[paramName] = d.data.data;

              if (!panel.chartOptions.searchQuery || !panel.chartOptions.timeWindow || panel.chartOptions.timeWindow.length !== 2) {
                return;
              }

              const q = panel.chartOptions.searchQuery + (panel.chartOptions.searchQueryExtention || '');
              const startTime = panel.chartOptions.timeWindow[0];
              const endTime = panel.chartOptions.timeWindow[1];

              const ws = this.getSocketConn();
              ws.subscribe(
                msg => {
                  if (msg && msg.data) {
                    msg.data[0]['__header__'].map(c => {
                      c["field"] = c.name
                      c["header"] = c.name
                      c["sortable"]  = true
                      c["searchable"] = false
                      c["resizable"] = true
                      c["width"] = "25%"
                    });

                    panel.chartOptions.chartConfig.columns = msg.data[0]['__header__'];

                    panel.chartOptions.dataset.source = msg.data.slice(1).filter(d => d[panel.chartOptions.chartConfig.columns[0]['field']] != null);
                  }
                }, error => {

                }, () => {

                }
              );

              ws.next({query: q, from: startTime, to: endTime, pivot: false, realTime: panel.chartOptions.realtime, database: 'sonicwall'})
            })
          })
        })
      }
    });
  }


  private updateData() {
    const dates = [
      {ts: "2020-02-09T12:01:00.000Z", val: 1},
      {ts: "2020-02-09T12:02:00.000Z", val: 2},
      {ts: "2020-02-09T12:01:00.000Z", val: 30},
      {ts: "2020-02-09T12:01:00.000Z", val: 10},
      {ts: "2020-02-09T12:02:00.000Z", val: 50, val2: 30},
      {ts: "2020-02-09T12:03:00.000Z", val: 40},
      {ts: "2020-02-09T12:01:00.000Z", val: 50, val2: 3},
      {ts: "2020-02-09T12:02:00.000Z", val: 30},
      {ts: "2020-02-09T12:00:00.000Z", val: 50},
      {ts: "2020-02-09T12:05:00.000Z", val2: 50}
    ]

    // dates.forEach(date => {
    //   this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source.push(
    //     {"timestamp": date["ts"], "sensor1": date["val"]});
    // })

    let i = 0;
    let test = [];
    //let sub;


      this.sub1 = setInterval(() => {
        this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source.push(
          {"timestamp": new Date(), "sensor1": 10, "sensor2": 100}); i++;
      }, 1000)

     setTimeout(() => {
       clearInterval(this.sub1)
       this.myDashServ.dashboards[0].data[0].chartOptions.loadStatus = "Completed"
      }, 12000);

    // setTimeout(() => {
    //   this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source =[{}];
    //   this.myDashServ.dashboards[0].data[0].chartOptions.loadStatus = "Completed"
    //  }, 4000)

    // let sub2 = setTimeout(() => {
    //   this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source.push(
    //     {"timestamp": new Date(), "sensor1": Math.random() * 10, "sensor2": Math.random() * 100}); i++;
    // }, 45000);

    // let sub2 = setInterval(() => {
    //   this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source.push(
    //     {"timestamp": "2020-02-09T12:05:00.000Z", "sensor1": 100, "sensor2": 0});
    // }, 11000);

     //setTimeout(() => {clearInterval(sub2)}, 12000);


    //  setTimeout(() => {
    //   this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source = [{"timestamp": new Date(), "sensor1": 10, "sensor2": 100}]
    //   this.myDashServ.dashboards[0].data[0].chartOptions.loadStatus = "Completed"
    //  }, 9000)

    // this.sub2 = setInterval(() => {
    //   this.myDashServ.dashboards[0].data[2].chartOptions.dataset.source.push(
    //     this.generateRandomMessage())
    // }, 1000)

    // setTimeout(() => {
    //   clearInterval(this.sub2);
    //   this.myDashServ.dashboards[0].data[2].chartOptions.loadStatus = "Completed"
    // }, 12000)
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

    /*panel.chartOptions.dataset.source = [{}];

    if (panel.panelId === 'rawTable') {
      //clearInterval(this.sub2);

      this.sub2 = setInterval(() => {
        this.myDashServ.dashboards[0].data[2].chartOptions.dataset.source.push(
          this.generateRandomMessage())
      }, 1000)

      setTimeout(() => {
        clearInterval(this.sub2);
        this.myDashServ.dashboards[0].data[2].chartOptions.loadStatus = "Completed"
      }, 12000)
    } else if (panel.panelId === 'barchart') {
      clearInterval(this.sub1);

      this.sub1 = setInterval(() => {
        this.myDashServ.dashboards[0].data[0].chartOptions.dataset.source.push(
          {"timestamp": new Date(), "sensor1": 10, "sensor2": 100});
      }, 1000)

      setTimeout(() => {
        clearInterval(this.sub1)
        this.myDashServ.dashboards[0].data[0].chartOptions.loadStatus = "Completed"
      }, 12000);
    }*/
  }

  myCallback() {
    alert('Updated');
  }

  generateRandomMessage() {
    return {message: "This is a random message with id: " + Math.random(), host: 'host1', timestamp: new Date()}
  }

  private getSocketConn(): WebSocketSubject<any> {
    return webSocket({
      url: "ws://localhost:5001",
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
