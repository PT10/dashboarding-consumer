import { HttpClientModule } from '@angular/common/http';
import { DataService } from './data.service';
import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { FormsModule } from '@angular/forms';
import { DashboardModule } from '@bolt-analytics/ngx-dashboard';
import { NgxEchartsModule } from 'ngx-echarts';
import { CalendarModule } from 'primeng/calendar';
import { TableModule } from 'primeng/table';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';

import * as echarts from 'echarts';

@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    HttpClientModule,
    DashboardModule,
    TableModule,
    NgxEchartsModule.forRoot({
      echarts,
    }),
    CalendarModule
  ],
  providers: [DataService],
  bootstrap: [AppComponent]
})
export class AppModule { }
