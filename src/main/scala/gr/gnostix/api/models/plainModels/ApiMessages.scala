package gr.gnostix.api.models.plainModels

/**
 * Created by rebel on 24/11/14.
 */

object ApiMessages {

  def generalError = Map("status" -> 400, "message" -> "Something went wrong")

  def generalErrorOnData = Map("status" -> 400, "message" -> "Error on data!")

  def generalErrorWithMessage(message: String) = Map("status" -> 400, "message" -> message)

  def generalSuccess(objName: String, data: Any) =  Map("status" -> 200, "message" -> "All good", "payload" -> Map(objName -> data))

  def generalSuccessOneParam(data: Any) =  Map("status" -> 200, "message" -> "All good", "payload" -> data)

  def generalSuccessNoData =  Map("status" -> 200, "message" -> "All good", "payload" -> List())
  
  def generalSuccessWithMessage(message: String) = Map("status" -> 200, "message" -> message)

}
