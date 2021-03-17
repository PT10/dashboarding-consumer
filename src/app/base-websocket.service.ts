import { Injectable } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';

@Injectable()
export class BaseWebsocketService {

  constructor() {
    this.connect();
    this.sendHeartbit();
   }

  private subject: WebSocketSubject<any>;

  public connect(): WebSocketSubject<any> {
    if (this.subject && !this.subject.closed) {
      return this.subject;
    }

    this.subject = webSocket({
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
    });

    return this.subject;
  }

  private sendHeartbit() {
    setInterval(() => this.send('pingSig'), 10000);
  }

  public send(msg: any) {
    this.subject.next(msg);
  }

  public disconnect() {
    this.subject.complete();
  }
}
