package gr.gnostix.api.models

/**
 * Created by rebel on 24/11/14.
 */

// empty object
case class Payload2
object ApiMessages {

  def generalError = Map("status" -> 400, "message" -> "Something went wrong")

  def generalSuccess(objName: String, data: Any) =  Map("status" -> 200, "message" -> "All good", "payload" -> Map(objName -> data))

  def generalSuccessOneParam(data: Any) =  Map("status" -> 200, "message" -> "All good", "payload" -> data)

  def generalSuccessNoData =  Map("status" -> 200, "message" -> "All good", "payload" -> List())

}
