package csci.ryan_williams.masters.entity

import java.time._;
import java.time.temporal._;
import java.lang._;

object EventStatus {
  def UNKNOWN = 0;
  def SCHEDULED = 1;
  def INPROGRESS = 2;
  def CANCELED = 3;
  def COMPLETE = 4;
}

class Event(task_id: TaskId, resourceIds: Array[ResourceId], start: Instant, end: Instant) {
  private var _taskId: TaskId = task_id;
  private var _resourceIds: Array[ResourceId] = resourceIds;
  private var _start: Instant = start;
  private var _end: Instant = end;
  private var _status: EventStatus = EventStatus.UNKNOWN;
  
  def EventId: EventId = { this._taskId; }
  def EventId_(eid: EventId) = { this._taskId = eid; }
  
  def TaskId: TaskId = { this._taskId; }
  def TaskId_(tid: TaskId) = { this._taskId = tid; }
  
  def ResourceIds: Array[ResourceId] = { this._resourceIds; }
  def ResourceIds_(ids: Array[ResourceId]) = { this._resourceIds = ids; }
  
  def Start: Instant = { this._start; }
  def Start_(startInstant: Instant) = { this._start = startInstant; }
  
  def End: Instant = { this._end; }
  def End_(endInstant: Instant) = { this._end = endInstant; }
  
  def Status: EventStatus = { this._status; }
  def Status_(status: EventStatus) = { this._status = status }
}