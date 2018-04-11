package controllers

import javax.inject._

import actor.KsqlWebSocketActor
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.Materializer
import play.api.Configuration
import play.api.libs.streams.ActorFlow
import play.api.libs.ws.WSClient
import play.api.mvc._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class Application @Inject()
    (ws: WSClient, cc: ControllerComponents, cfg: Configuration)
    (implicit system: ActorSystem, mat: Materializer)
    extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index())
  }

  def ksql(): WebSocket = WebSocket.accept[String,String] { _: RequestHeader =>
    ActorFlow.actorRef { webSocketClient: ActorRef =>
      KsqlWebSocketActor.props(webSocketClient, ws, cfg)
    }
//    Flow[String].
//      mapAsync(1) { msg: String =>
//        println(s"Received ${msg}")
//        ws.
//          url(s"${cfg.get[String]("ksql.service.base.url")}/query").
//          withMethod("POST").
//          withHttpHeaders(
//            "Content-Type" -> "application/json; charset=utf-8"
//          ).
//          withBody(msg).
//          stream().
//          map { resp: WSResponse => resp.bodyAsSource }
//      }.
//      flatMapConcat(identity).
//      map(_.utf8String.trim).
//      filter(_.nonEmpty).
//      map { line =>
//        println(s"Sending ${line}")
//        line
//      }
  }
}
