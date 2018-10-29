import { Component, OnInit, Input } from '@angular/core';
import { PlanService } from 'src/app/services/plan/plan.service';
import { NodeComponent } from './node/node.component';

@Component({
  selector: 'app-visualizer',
  templateUrl: './visualizer.component.html',
  styleUrls: ['./visualizer.component.scss']
})
export class VisualizerComponent implements OnInit {

  @Input() plan: any;
  @Input() query: string;

  constructor(private planService: PlanService) {
  }

  ngOnInit() {
    this.planService.augmentStatistics(this.plan);
    NodeComponent.nodes = [];
  }

  deselectPlan() {
    this.planService.selectPlan(undefined);
  }

}
