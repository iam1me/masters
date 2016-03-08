package csci.ryan_williams.masters.entity

class Schedule(id: ScheduleId, eventIds: Array[EventId]) {
  private var _id: ScheduleId = id;
  private var _eventIds: Array[EventId] = eventIds;
  
  def Id: ScheduleId = this._id;
  def Id_(sid: ScheduleId) = { this._id = sid; }
  
  def EventIds: Array[EventId] = this._eventIds;
  def EventIds_(eventIds: Array[EventId]) = { this._eventIds = eventIds; }
}