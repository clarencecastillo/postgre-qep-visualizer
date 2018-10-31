import { Component, OnInit, Input } from '@angular/core';
import { NodeComponent } from '../visualizer/node/node.component';
import { PlanService } from 'src/app/services/plan/plan.service';

@Component({
  selector: 'app-inspector',
  templateUrl: './inspector.component.html',
  styleUrls: ['./inspector.component.scss']
})
export class InspectorComponent implements OnInit {

  private readonly HANDLED_ATTRIBUTES = ['Plans'];
  private readonly CALCULATED_ATTRIBUTES = ['Actual Duration'];

  @Input() plan: any;
  @Input() query: string;
  attributes: string[];

  constructor(private planService: PlanService) { }

  ngOnInit() {
    this.attributes = Object.keys(this.plan)
      .filter(key => !(this.HANDLED_ATTRIBUTES.includes(key) || this.CALCULATED_ATTRIBUTES.includes(key)));
  }

  select(index) {
    NodeComponent.nodes.forEach(node => {
      node.selected = false;
      node.selectedPlanIndex = -1;
    });
    this.planService.selectPlan(this.plan['Plans'][index]);
  }

}
