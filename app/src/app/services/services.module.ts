import { NgModule } from '@angular/core';
import { PlanService } from './plan/plan.service';
import { HttpClientModule } from '@angular/common/http';

const SERVICES = [
  PlanService
];

@NgModule({
  imports: [
    HttpClientModule
  ],
  providers: [
    ...SERVICES
  ]
})
export class ServicesModule { }
