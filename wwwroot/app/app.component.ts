import {Component} from 'angular2/core';

import { TaskEntryComponent } from './task-entry.component';

@Component({
    selector: 'my-app',
    template: `<task-entry></task-entry>`,
	directives: [TaskEntryComponent]
	
})
export class AppComponent { }