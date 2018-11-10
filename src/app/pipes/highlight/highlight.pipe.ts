import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'highlight'
})
export class HighlightPipe implements PipeTransform {

  transform(query: string, components: QueryComponent[]): any {

    components.sort((component1, component2) => component2.start - component1.start);

    components.forEach(component => {
      const pre_text = query.slice(0, component.start);
      const match = query.slice(component.start, component.end);
      const post_text = query.slice(component.end);
      query = `${pre_text}<mark>${match}</mark>${post_text}`;
    });

    return query;
  }

}

interface QueryComponent {
  start: number;
  end: number;
  match: string;
}
