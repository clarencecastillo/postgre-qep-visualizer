import { Component, OnInit, Input, OnDestroy } from '@angular/core';
import { PlanService } from 'src/app/services/plan/plan.service';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-node',
  templateUrl: './node.component.html',
  styleUrls: ['./node.component.scss']
})
export class NodeComponent implements OnInit, OnDestroy {

  static nodes: NodeComponent[] = [];
  @Input() plans: any[];

  highlighted = false;
  highlightedPlanIndex = -1;

  selected = false;
  selectedPlanIndex = -1;

  private unsubscribe: Subject<void> = new Subject();

  constructor(private planService: PlanService) {
    this.planService.onSelectedPlanChange.pipe(takeUntil(this.unsubscribe)).subscribe(plan => {
      if (!plan && this.selected) {
        this.selected = false;
        this.selectedPlanIndex = -1;
        return;
      }

      if (this.plans && this.plans.includes(plan)) {
        this.selected = true;
        this.selectedPlanIndex = this.plans.indexOf(plan);
      }
    });
  }

  ngOnInit() {
    NodeComponent.nodes.push(this);
  }

  select(index) {
    NodeComponent.nodes.forEach(node => {
      node.selected = false;
      node.selectedPlanIndex = -1;
    });
    this.planService.selectPlan(this.plans[index]);
  }

  highlight(index) {
    NodeComponent.nodes.forEach(node => {
      node.highlighted = false;
      node.highlightedPlanIndex = -1;
    });
    this.highlighted = true;
    this.highlightedPlanIndex = index;
  }

  unhighlight() {
    this.highlighted = false;
    this.highlightedPlanIndex = -1;
  }

  ngOnDestroy() {
    this.unsubscribe.next();
    this.unsubscribe.complete();
  }

}
