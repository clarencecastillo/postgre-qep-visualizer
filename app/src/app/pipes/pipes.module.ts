import { NgModule } from '@angular/core';
import { HighlightPipe } from './highlight/highlight.pipe';

const PIPES = [
  HighlightPipe
];

@NgModule({
  declarations: [
    ...PIPES
  ],
  exports: [
    ...PIPES
  ],
  providers: [
    ...PIPES
  ]
})
export class PipesModule { }
