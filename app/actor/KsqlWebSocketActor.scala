package actor

import actor.KsqlWebSocketActor.PerQueryActor
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, Materializer}
import com.fasterxml.jackson.core.JsonParseException
import play.api.Configuration
import play.api.libs.json.Json
import play.api.libs.ws.{WSClient, WSResponse}

import scala.util.{Failure, Success, Try}

object KsqlWebSocketActor {
  def props(webSocketClient: ActorRef, ws: WSClient, cfg: Configuration): Props =
    Props(new KsqlWebSocketActor(webSocketClient, ws, cfg))

  object PerQueryActor {
    // Internal messages
    case class KsqlResponse(bodyPart: String)
    case object KsqlQueryDone

    def props(query: String, webSocketClient: ActorRef, ws: WSClient, cfg: Configuration): Props =
      Props(new PerQueryActor(query, webSocketClient, ws, cfg))
  }
  class PerQueryActor(query: String, webSocketClient: ActorRef, ws: WSClient, cfg: Configuration)
      extends Actor with ActorLogging {
    import PerQueryActor._
    import context.dispatcher

    implicit val materializer: Materializer = ActorMaterializer()

    log.info(s"Received: ${query}")
    ws.url(s"${cfg.get[String]("ksql.service.base.url")}/query").
      withMethod("POST").
      withHttpHeaders(
        "Content-Type" -> "application/json; charset=utf-8"
      ).
      withBody(query).
      stream().
      filter(_.status == 200).
      recoverWith {
        case e: NoSuchElementException =>
          ws.url(s"${cfg.get[String]("ksql.service.base.url")}/ksql").
            withMethod("POST").
            withHttpHeaders(
              "Content-Type" -> "application/json; charset=utf-8"
            ).
            withBody(query).
            stream()
      }.
      pipeTo(self)
    context.become(awaitingServiceResponse)

    private lazy val awaitingServiceResponse: Receive = {
      case resp: WSResponse =>
        resp.bodyAsSource.
          map(_.utf8String.trim).
          filter(_.nonEmpty).
          runForeach { line: String =>
            self ! KsqlResponse(line)
          }.
          map { _ => KsqlQueryDone }.
          pipeTo(self)
        context.become(processingResponseBody(""))
    }

    private def processingResponseBody(incompleteBody: String): Receive = {
      case KsqlResponse(bodyPart: String) =>
        val line = incompleteBody + bodyPart
        Try(Json.parse(line)) match {
          case Success(_) =>
            log.info(s"Sending: ${line}")
            webSocketClient ! line
            context.become(processingResponseBody(""))

          case Failure(e: JsonParseException) =>
            context.become(processingResponseBody(incompleteBody + bodyPart))

          case Failure(t: Throwable) => throw t
        }

      case KsqlQueryDone =>
        log.info("Done processing KSQL service response body.")
        context.stop(self)
    }

    override def receive: Receive = PartialFunction.empty
  }
}
class KsqlWebSocketActor(webSocketClient: ActorRef, ws: WSClient, cfg: Configuration)
    extends Actor with ActorLogging {
  private val idle: Receive = {
    case query: String =>
      context.become(
        active(
          context.watch(
            context.actorOf(
              PerQueryActor.props(query, webSocketClient, ws, cfg)
            )
          )
        )
      )
  }

  private def active(queryActor: ActorRef): Receive = {
    case query: String =>
      context.stop(queryActor) // TODO send message and perform clean stop
      context.become(awaitingQueryTermination(query, queryActor))

    case Terminated(`queryActor`) =>
      context.become(idle)
  }

  private def awaitingQueryTermination(nextQuery: String, queryActor: ActorRef): Receive = {
    case query: String =>
      context.become(awaitingQueryTermination(query, queryActor))

    case Terminated(`queryActor`) =>
      context.become(idle)
      self ! nextQuery
  }

  override def unhandled(message: Any): Unit = {
    log.warning(s"Unhandled message ${message}.")
    super.unhandled(message)
  }

  override val receive: Receive = idle
}
