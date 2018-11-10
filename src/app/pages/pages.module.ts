import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HomeComponent } from './home/home.component';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { NavigationComponent } from './navigation/navigation.component';
import { AboutComponent } from './about/about.component';
import { IdeComponent } from './home/ide/ide.component';
import { FormsModule } from '@angular/forms';
import { MomentModule } from 'ngx-moment';
import { GraphComponent } from './graph/graph.component';
import { VisualizerComponent } from './graph/visualizer/visualizer.component';
import { NodeComponent } from './graph/visualizer/node/node.component';
import { FooterComponent } from './footer/footer.component';
import { RouterModule } from '@angular/router';
import { InspectorComponent } from './graph/inspector/inspector.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { PlanComponent } from './graph/visualizer/node/plan/plan.component';
import { PipesModule } from '../pipes/pipes.module';

const COMPONENTS = [
  HomeComponent,
  NavigationComponent,
  AboutComponent,
  IdeComponent,
  VisualizerComponent,
  NodeComponent,
  GraphComponent,
  FooterComponent,
  InspectorComponent,
  PlanComponent
];

@NgModule({
  declarations: [
    ...COMPONENTS
  ],
  imports: [
    CommonModule,
    FormsModule,
    NgbModule,
    MomentModule,
    RouterModule,
    BrowserAnimationsModule,
    PipesModule
  ],
  exports: [
    NavigationComponent,
    FooterComponent
  ]
})
export class PagesModule { }
