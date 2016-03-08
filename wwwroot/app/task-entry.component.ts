import { Component } from 'angular2/core';

@Component({
	selector: 'task-entry',
	templateUrl: 'templates/task-entry.html',
})
export class TaskEntryComponent
{
	/** Fields **/
	private _taskId: number = 25;
	private _desc: string = "This is a description";
	private _requiredResourceCodes: Array<string> = new Array<string>();
	
	/** Constructor **/
	constructor()
	{
		this._requiredResourceCodes.push("resource1");
		this._requiredResourceCodes.push("resource2");
	}
	
	/** Properties **/
	get TaskId() { return this._taskId; }
	set TaskId(id: number) { this._taskId = id; }
	
	get Description() { return this._desc; }
	set Description(desc: string) { this._desc = desc; }
	
	get RequiredResourceCodes() { return this._requiredResourceCodes; }
	
	/** Events **/
	onAddResource() {
		var resourceName = "resource" + this._requiredResourceCodes.length;
		this._requiredResourceCodes.push(resourceName);
	}
}

