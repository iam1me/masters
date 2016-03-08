package csci.ryan_williams.masters.entity

class Resource(rid: ResourceId, code: ResourceCode, rate: Double) {
  private var _id: ResourceId = rid;
  private var _code: ResourceCode = code;
  private var _hourlyRate: Double = rate;
  
  def Id: ResourceId = { this._id; }
  def Id_(rid: ResourceId) = { this._id = rid; }
  
  def Code: ResourceCode = { this._code; }
  def Code_(code: ResourceCode) = { this._code = code; }
  
  def HourlyRate: Double = { this._hourlyRate; }  
  def HourlyRate_(rate: Double) { this._hourlyRate = rate; } 
    
}