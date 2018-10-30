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

  selectedPlan: any;

  augmentedPlan: any;
  formattedQuery: string;

  private unsubscribe: Subject<void> = new Subject();

  constructor(private planService: PlanService) {
    this.planService.onSelectedPlanChange.pipe(takeUntil(this.unsubscribe)).subscribe(plan => {
      this.selectedPlan = plan;
    });
  }

  ngOnInit() {
    const plan = this.planService.getExecutionPlan();
    const query = this.planService.getQuery();
    this.planService.getAugmentedPlanStatistics(plan, query).then(response => {
      this.augmentedPlan = response['plan'];
      this.formattedQuery = response['query'];
    });
  }

  ngOnDestroy() {
    this.unsubscribe.next();
    this.unsubscribe.complete();
  }

}
