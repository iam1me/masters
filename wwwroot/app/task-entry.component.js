System.register(['angular2/core'], function(exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
        var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    };
    var __metadata = (this && this.__metadata) || function (k, v) {
        if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
    };
    var core_1;
    var TaskEntryComponent;
    return {
        setters:[
            function (core_1_1) {
                core_1 = core_1_1;
            }],
        execute: function() {
            TaskEntryComponent = (function () {
                /** Constructor **/
                function TaskEntryComponent() {
                    /** Fields **/
                    this._taskId = 25;
                    this._desc = "This is a description";
                    this._requiredResourceCodes = new Array();
                    this._requiredResourceCodes.push("resource1");
                    this._requiredResourceCodes.push("resource2");
                }
                Object.defineProperty(TaskEntryComponent.prototype, "TaskId", {
                    /** Properties **/
                    get: function () { return this._taskId; },
                    set: function (id) { this._taskId = id; },
                    enumerable: true,
                    configurable: true
                });
                Object.defineProperty(TaskEntryComponent.prototype, "Description", {
                    get: function () { return this._desc; },
                    set: function (desc) { this._desc = desc; },
                    enumerable: true,
                    configurable: true
                });
                Object.defineProperty(TaskEntryComponent.prototype, "RequiredResourceCodes", {
                    get: function () { return this._requiredResourceCodes; },
                    enumerable: true,
                    configurable: true
                });
                /** Events **/
                TaskEntryComponent.prototype.onAddResource = function () {
                    var resourceName = "resource" + this._requiredResourceCodes.length;
                    this._requiredResourceCodes.push(resourceName);
                };
                TaskEntryComponent = __decorate([
                    core_1.Component({
                        selector: 'task-entry',
                        templateUrl: 'templates/task-entry.html',
                    }), 
                    __metadata('design:paramtypes', [])
                ], TaskEntryComponent);
                return TaskEntryComponent;
            }());
            exports_1("TaskEntryComponent", TaskEntryComponent);
        }
    }
});
//# sourceMappingURL=task-entry.component.js.map