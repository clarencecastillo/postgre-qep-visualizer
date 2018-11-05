import { Component, OnInit } from '@angular/core';
import { SAMPLE_EXECUTION_PLAN, SAMPLE_QUERY } from 'src/app/services/plan/sample-plan';
import { PlanService } from 'src/app/services/plan/plan.service';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss']
})
export class HomeComponent implements OnInit {

  planText: string;
  query: string;
  error: string;

  constructor(private planService: PlanService) { }

  ngOnInit() {
    this.onPlanTextChange(this.planText);
    this.query = this.planService.getQuery();
    const savedPlanText = this.planService.getExecutionPlanText();
    if (savedPlanText) {
      this.planText = JSON.stringify([JSON.parse(savedPlanText)], null, 2);
    }
  }

  onPlanTextChange(planText: string) {
    if (planText) {
      try {
        this.planService.setExecutionPlan(planText);
        this.planText = planText;
        this.error = undefined;
      } catch (e) {
        this.error = e.message;
      }
    }
  }

  onSubmit() {
    this.planService.setQuery(this.query);
  }

  loadSample() {
    this.onPlanTextChange(SAMPLE_EXECUTION_PLAN);
    this.query = SAMPLE_QUERY;
  }

}
