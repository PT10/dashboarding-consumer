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
import {ToastModule} from 'primeng/toast';

import * as echarts from 'echarts';
import { MessageService } from 'primeng/components/common/messageservice';

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
    ToastModule,
    NgxEchartsModule.forRoot({
      echarts,
    }),
    CalendarModule
  ],
  providers: [DataService, MessageService],
  bootstrap: [AppComponent]
})
export class AppModule { }
