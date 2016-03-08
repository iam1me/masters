package csci.ryan_williams.masters.entity

import java.time._;
import java.time.temporal._;
import scala.collection._;
import scala.collection.generic._;
import scala.collection.mutable._;

class Task(task_id: TaskId, desc: String, resources: Array[ResourceCode]) {
  
  private var _id: TaskId = task_id;
  private var _description: String = desc;
  private var _requiredResourceCodeList: Array[ResourceCode] = resources;
  
  
  /*
  // duration is how long the Task needs run for
  private var _duration: Duration = null;
  
  // dueDate is when the Task needs to be completed by
  private var _dueDate: java.time.Instant = null;
  
  // availabilityDate is the earliest instant at which the Task maybe processed
  private var _availabilityDate: Instant = null;
  */
  
  def Id: TaskId = { return this._id; }
  def Id_(id: TaskId) = { this._id = id; }
  
  def Description: String = { return this._description; }
  def Description_(descStr: String) = { this._description = descStr; }
  
  // allow the availability of resources dictate when the 
  // task is to be performed (?)
  def RequiredResources: Array[ResourceCode] = {
    return this._requiredResourceCodeList;
  }
  
  def RequiredResources(resourceCodes: Array[ResourceCode]) = {
    this._requiredResourceCodeList = resourceCodes;    
  }
}