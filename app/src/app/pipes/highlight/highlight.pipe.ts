import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'highlight'
})
export class HighlightPipe implements PipeTransform {

  transform(value: any, args: any): any {
    //  'gi' for case insensitive and can use 'g' if you want the search to be case sensitive.
    const sanitised = args[0].replace('(', '\\(').replace(')', '\\)');
    const re = new RegExp(sanitised, 'gi');
    return value.replace(re, `<mark>${args[0]}</mark>`);
}

}
