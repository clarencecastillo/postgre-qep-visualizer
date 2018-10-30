import { NgModule } from '@angular/core';
import { PlanService } from './plan/plan.service';

const SERVICES = [
  PlanService
];

@NgModule({
  providers: [
    ...SERVICES
  ]
})
export class ServicesModule { }
