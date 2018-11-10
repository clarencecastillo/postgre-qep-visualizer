import { Component, OnInit, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';

@Component({
  selector: 'app-ide',
  templateUrl: './ide.component.html',
  styleUrls: ['./ide.component.scss']
})
export class IdeComponent implements OnInit, OnChanges {

  @Input() error: string;

  @Input() text: string;
  @Output() textChange: EventEmitter<string> = new EventEmitter();

  linesCount = 0;

  constructor() { }

  ngOnInit() {
    this.countLines();
  }

  onTextChange() {
    this.countLines();
    this.textChange.emit(this.text);
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['text']) {
      this.onTextChange();
    }
  }

  private countLines() {
    this.linesCount = this.text ? this.text.split(/\r\n|\r|\n/).length : 0;
  }

}
