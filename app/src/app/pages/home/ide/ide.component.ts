import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'app-ide',
  templateUrl: './ide.component.html',
  styleUrls: ['./ide.component.scss']
})
export class IdeComponent implements OnInit {

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

  private countLines() {
    this.linesCount = this.text ? this.text.split(/\r\n|\r|\n/).length : 0;
  }

}
