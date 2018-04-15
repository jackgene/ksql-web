package actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status, Terminated}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, Materializer}
import com.fasterxml.jackson.core.JsonParseException
import play.api.Configuration
import play.api.libs.json.Json
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object KsqlWebSocketActor {
  def props(webSocketClient: ActorRef, ws: WSClient, cfg: Configuration): Props =
    Props(new KsqlWebSocketActor(webSocketClient, ws, cfg))

  private object ResponseBatchingActor {
    // Internal messages
    case object SendBatch

    def props(webSocketClient: ActorRef): Props =
      Props(new ResponseBatchingActor(webSocketClient))

    val BatchPeriod: FiniteDuration = 20.milliseconds
  }
  private class ResponseBatchingActor(webSocketClient: ActorRef) extends Actor with ActorLogging {
    import ResponseBatchingActor._
    import context.dispatcher

    private def extractJsonArrayElems(maybeJsonArray: String): String = {
      if (!maybeJsonArray.startsWith("[") || ! maybeJsonArray.endsWith("]")) maybeJsonArray
      else maybeJsonArray.substring(1, maybeJsonArray.length - 1)
    }

    private val sending: Receive = {
      case response: String =>
        log.debug("Sending response unbatched.")
        webSocketClient ! s"[${extractJsonArrayElems(response)}]"
        context.become(batching(List()))
        context.system.scheduler.scheduleOnce(BatchPeriod, self, SendBatch)
    }

    private def batching(bufferedResponses: List[String]): Receive = {
      case response: String =>
        context.become(batching(extractJsonArrayElems(response) :: bufferedResponses))

      case SendBatch =>
        bufferedResponses match {
          case Nil =>
            context.become(sending)

          case nonEmptyResponses: List[String] =>
            log.debug(s"Sending ${nonEmptyResponses.size} response(s) in a batch.")
            webSocketClient ! nonEmptyResponses.mkString("[", ",", "]")
            context.become(batching(List()))
            context.system.scheduler.scheduleOnce(BatchPeriod, self, SendBatch)
        }
    }

    override val receive: Receive = sending
  }

  private object PerQueryActor {
    // Internal messages
    case class KsqlResponse(bodyPart: String)
    case object KsqlQueryDone
    case object Stop

    def props(query: String, webSocketClient: ActorRef, ws: WSClient, cfg: Configuration): Props =
      Props(new PerQueryActor(query, webSocketClient, ws, cfg))
  }
  private class PerQueryActor(query: String, webSocketClient: ActorRef, ws: WSClient, cfg: Configuration)
      extends Actor with ActorLogging {
    import PerQueryActor._
    import context.dispatcher

    implicit val materializer: Materializer = ActorMaterializer()

    log.debug(s"Received: ${query}")
    private def sendKsqlServiceRequest(): Unit = {
      ws.url(s"${cfg.get[String]("ksql.service.base.url")}/query").
        withMethod("POST").
        withHttpHeaders(
          "Content-Type" -> "application/json; charset=utf-8"
        ).
        withBody(query).
        stream().
        filter(_.status == 200).
        recoverWith {
          case _: NoSuchElementException =>
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
    }

    private val awaitingServiceResponse: Receive = {
      case resp: WSResponse =>
        resp.bodyAsSource.
          map(_.utf8String.trim).
          filter(_.nonEmpty).
          runForeach { line: String =>
            self ! KsqlResponse(line)
          }.
          map { _ => KsqlQueryDone }.
          pipeTo(self)
        context.become(processingResponseBody("", done = false))

      case Stop =>
        log.debug("Received stop request.")
        context.stop(self)

      case Status.Failure(t: Throwable) =>
        val msg = "Failed to connect to KSQL service, make sure the service is running, and ksql.service.base.url is set correctly."
        log.error(t, msg)
        // Mimics KSQL's error message JSON
        webSocketClient ! s"""{"error":{"errorMessage":{"message":"${msg}: ${t.getMessage}"}}}"""
    }

    private def processingResponseBody(incompleteBody: String, done: Boolean): Receive = {
      case KsqlResponse(bodyPart: String) =>
        val line = incompleteBody + bodyPart
        Try(Json.parse(line)) match {
          case Success(_) =>
            log.debug(s"Sending: ${line}")
            webSocketClient ! line
            context.become(
              processingResponseBody("", !line.startsWith("""{"row":{"columns":["""))
            )

          case Failure(_: JsonParseException) =>
            context.become(
              processingResponseBody(incompleteBody + bodyPart, done)
            )

          case Failure(t: Throwable) => throw t
        }

      case KsqlQueryDone =>
        if (!done) {
          log.info("KSQL service response terminated when more data was expected. Resending request.")
          sendKsqlServiceRequest()
        } else {
          log.info("Done processing KSQL service response body.")
          context.stop(self)
        }

      case Stop =>
        log.debug("Received stop request.")
        context.stop(self)
    }

    override def receive: Receive = PartialFunction.empty

    override def unhandled(message: Any): Unit = {
      log.warning(s"Unhandled message:\n${message}")
    }

    sendKsqlServiceRequest()
  }
}
class KsqlWebSocketActor(webSocketClient: ActorRef, ws: WSClient, cfg: Configuration)
    extends Actor with ActorLogging {
  import KsqlWebSocketActor._

  log.info("Starting KSQL session.")
  private val batchingWebSocketClient: ActorRef =
    context.actorOf(ResponseBatchingActor.props(webSocketClient), "batching")
  private val idSeq: Iterator[Int] = Iterator.from(0)

  private val idle: Receive = {
    case "{}" =>
      // Keep alive - No-op
      log.debug("Received keep-alive.")

    case """{"cmd":"stop"}""" =>
      // No query running. No-op
      log.debug("Received stop request with no active query. Ignoring.")

    case query: String =>
      context.become(
        active(
          context.watch(
            context.actorOf(
              PerQueryActor.props(query, batchingWebSocketClient, ws, cfg),
              s"query-${idSeq.next}"
            )
          )
        )
      )
  }

  private def active(queryActor: ActorRef): Receive = {
    case "{}" =>
      // Keep alive - No-op
      log.debug("Received keep-alive.")

    case """{"cmd":"stop"}""" =>
      queryActor ! PerQueryActor.Stop
      context.become(awaitingQueryTermination(None, queryActor))

    case query: String =>
      queryActor ! PerQueryActor.Stop
      context.become(awaitingQueryTermination(Some(query), queryActor))

    case Terminated(`queryActor`) =>
      context.become(idle)
  }

  // TODO this may not be necessary if the existing `context.stop(queryActor)` above is stopping the query cleanly
  private def awaitingQueryTermination(nextQueryOpt: Option[String], queryActor: ActorRef): Receive = {
    case "{}" =>
      // Keep alive - No-op
      log.debug("Received keep-alive.")

    case """{"cmd":"stop"}""" =>
      context.become(awaitingQueryTermination(None, queryActor))

    case query: String =>
      context.become(awaitingQueryTermination(Some(query), queryActor))

    case Terminated(`queryActor`) =>
      context.become(idle)
      for (nextQuery: String <- nextQueryOpt) {
        self ! nextQuery
      }
  }

  override def unhandled(message: Any): Unit = {
    log.warning(s"Unhandled message\n${message}")
    super.unhandled(message)
  }

  override val receive: Receive = idle

  override def postStop(): Unit = {
    log.info("Stopping KSQL session.")
  }
}
