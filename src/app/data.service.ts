import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class DataService {
  dashboards: {name: string, data: any[], options: {}}[] = [];

  constructor(private http: HttpClient) {}

  getDashboardData() {
    //return this.http.get('/assets/searchDashboard_kafka_onlyRead.json');
    return this.http.get('/assets/searchDashboard_kafka.json');
  }

  getDashboards() {
    return JSON.parse(JSON.stringify(this.dashboards));
  }
}
