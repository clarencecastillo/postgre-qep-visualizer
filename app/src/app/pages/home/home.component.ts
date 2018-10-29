import { Component, OnInit } from '@angular/core';
import { SAMPLE_EXECUTION_PLAN, SAMPLE_QUERY } from 'src/app/services/plan/sample-plan';
import { PlanService } from 'src/app/services/plan/plan.service';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss']
})
export class HomeComponent implements OnInit {

  planText: string = SAMPLE_EXECUTION_PLAN;
  query: string = SAMPLE_QUERY;
  error: string;

  constructor(private planService: PlanService) { }

  ngOnInit() {
    this.onPlanTextChange(this.planText);
  }

  onPlanTextChange(planText: string) {
    try {
      this.planService.setExecutionPlan(planText);
      this.planText = planText;
      this.error = undefined;
    } catch (e) {
      this.error = e.message;
    }
  }

  onSubmit() {
    this.planService.setQuery(this.query);
  }

}
