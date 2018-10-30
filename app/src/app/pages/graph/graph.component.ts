import { Component, OnInit, OnDestroy } from '@angular/core';
import { PlanService } from 'src/app/services/plan/plan.service';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-graph',
  templateUrl: './graph.component.html',
  styleUrls: ['./graph.component.scss']
})
export class GraphComponent implements OnInit, OnDestroy {

  plan: any;
  query: string;
  selectedPlan: any;

  private unsubscribe: Subject<void> = new Subject();

  constructor(private planService: PlanService) {
    this.planService.onSelectedPlanChange.pipe(takeUntil(this.unsubscribe)).subscribe(plan => {
      this.selectedPlan = plan;
    });
  }

  ngOnInit() {
    this.plan = this.planService.getExecutionPlan();
    this.query = this.planService.getQuery();
  }

  ngOnDestroy() {
    this.unsubscribe.next();
    this.unsubscribe.complete();
  }

}
