import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class PlanService {

  private readonly STORAGE_EXECUTION_PLAN = 'execution-plan';
  private readonly STORAGE_QUERY = 'query';

  private executionPlan: any;
  private query: string;

  private selectedPlan: any;
  private selectedPlanChange: Subject<any>;
  readonly onSelectedPlanChange: Observable<any>;

  constructor() {

    this.selectedPlanChange = new Subject();
    this.onSelectedPlanChange = this.selectedPlanChange.asObservable();

    const savedExecutionPlan = localStorage.getItem(this.STORAGE_EXECUTION_PLAN);
    if (savedExecutionPlan) {
      this.setExecutionPlan(savedExecutionPlan, false);
    }

    const savedQuery = localStorage.getItem(this.STORAGE_QUERY);
    if (savedQuery) {
      this.setQuery(savedQuery, false);
    }
  }

  selectPlan(plan: any) {
    this.selectedPlan = plan;
    this.selectedPlanChange.next(plan);
  }

  getSelectedPlan() {
    this.selectedPlan = this.selectedPlan;
  }

  setExecutionPlan(executionPlan: string, save: boolean = true) {
    try {
      this.executionPlan = JSON.parse(executionPlan)[0];
      if (save) {
        localStorage.setItem(this.STORAGE_EXECUTION_PLAN, executionPlan);
      }
    } catch (e) {
      this.executionPlan = undefined;
      if (save) {
        localStorage.removeItem(this.STORAGE_EXECUTION_PLAN);
      }
      throw new Error('The JSON string contains errors');
    }
  }

  getExecutionPlan(): any {
    return this.executionPlan;
  }

  setQuery(query: string, save: boolean = true) {
    this.query = query;
    if (save) {
      localStorage.setItem(this.STORAGE_QUERY, query);
    }
  }

  getQuery(): string {
    return this.query;
  }

  augmentStatistics(executionPlan: any) {
    this.augmentPlanStatistics(executionPlan['Plan']);
  }

  augmentPlanStatistics(plan: any) {
    plan['Actual Duration'] = this.calculateActualDuration(plan);
    if (plan['Plans']) {
      plan['Plans'].forEach(subPlan => this.augmentPlanStatistics(subPlan));
    }
  }

  calculateActualDuration(plan): number {
    let actualDuration: number = plan['Actual Total Time'];
    if (plan['Plans']) {
      plan['Plans'].forEach(subPlan => {
        if (subPlan['Node Type'] !== 'CTE_Scan') {
          actualDuration -= subPlan['Actual Total Time'];
        }
      });
    }

    // time is reported for an invidual loop
    // actual duration must be adjusted by number of loops
    actualDuration = actualDuration * plan['Actual Loops'];
    return actualDuration;
  }

}

// export class Plan {
//   actual_loops: number;
//   actual_rows: number;
//   actual_startup_time: number;
//   actual_total_time: number;
//   io_read_time: number;
//   io_write_time: number;
//   local_dirtied_blocks: number;
//   local_hit_blocks: number;
//   local_read_blocks: number;
//   local_written_blocks: number;
//   node_type: string;
//   output: string[];
//   plan_rows: number;
//   plan_width: number;
//   plans: Plan[];
//   shared_dirtied_blocks: number;
//   shared_hit_blocks: number;
//   shared_written_blocks: number;
//   startup_cost: number;
//   temp_read_blocks: number;
//   temp_written_blocks: number;
//   total_cost: number;
// }

// export class ExecutionPlan {
//   execution_time: number;
//   plan: Plan;
//   planning_time: number;
// }
