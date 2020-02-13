module Ksql.Update exposing (update)

import Dict exposing (Dict)
import Dom.Scroll
import Json.Decode as Decode
import Ksql.Common exposing (..)
import Ksql.Port exposing (..)
import Random
import Regex exposing (Regex)
import Stream exposing (Stream, (:::))
import Task
import Time exposing (Time)
import WebSocket


maxDisplayedRows : Int
maxDisplayedRows = 5000


maxQueryHistoryItems : Int
maxQueryHistoryItems = 500


setStatementRegex : Regex
setStatementRegex =
  Regex.caseInsensitive (Regex.regex "^SET\\W+'((?:[^']|'')+)'\\W*=\\W*'((?:[^']|'')*)';$")


unsetStatementRegex : Regex
unsetStatementRegex =
  Regex.caseInsensitive (Regex.regex "^UNSET\\W+'((?:[^']|'')+)';$")

-- Update
-- JSON response from WebSocket
type Response
  -- Meta... responses are KSQL Web specific control messages
  = MetaConnected
  | MetaRawContentFollows String
  -- Everything else come from the KSQL REST API
  | StreamedRowResponse Row
  | StreamedTextResponse String
  | TableResponse Table
  | PropertiesResponse (Dict String String)
  | DescribeExtendedResponse ExtendedSchema
  | ExplainResponse ExecutionPlan
  | NotificationMessageResponse String
  | TableAndNotificationMessageResponse Table String
  | ErrorMessageResponse String


-- JSON decoder
responseDecoder : Decode.Decoder Response
responseDecoder =
  let
    -- Decodes {"ksqlWeb":...}
    ksqlWebRespDecoder : Decode.Decoder Response
    ksqlWebRespDecoder =
      Decode.field "ksqlWeb"
        (Decode.field "msg" Decode.string |> Decode.andThen
          ( \msg ->
            case msg of
              "connected" ->
                Decode.succeed MetaConnected

              "rawContentFollows" ->
                Decode.map MetaRawContentFollows (Decode.field "format" Decode.string)

              unsupported ->
                Decode.fail ("Unhandled ksqlWeb message: " ++ unsupported)
          )
        )


    columnDecoder : Decode.Decoder Column
    columnDecoder =
      let
        boolColumnDecoder : Decode.Decoder Column
        boolColumnDecoder = Decode.map BoolColumn Decode.bool

        numericColumnDecoder : Decode.Decoder Column
        numericColumnDecoder = Decode.map NumericColumn Decode.float

        stringColumnDecoder : Decode.Decoder Column
        stringColumnDecoder = Decode.map StringColumn Decode.string

        nullColumnDecoder : Decode.Decoder Column
        nullColumnDecoder = Decode.null NullColumn

        arrayColumnDecoder : Decode.Decoder Column
        arrayColumnDecoder = Decode.map ArrayColumn (Decode.list (Decode.lazy (\_ -> columnDecoder)))

        jsonColumnDecoder : Decode.Decoder Column
        jsonColumnDecoder = Decode.map JsonColumn (Decode.keyValuePairs (Decode.lazy (\_ -> columnDecoder)))
      in
        Decode.oneOf
        [ boolColumnDecoder
        , numericColumnDecoder
        , stringColumnDecoder
        , nullColumnDecoder
        , arrayColumnDecoder
        , jsonColumnDecoder
        ]


    rowRespDecoder : Decode.Decoder Response
    rowRespDecoder =
      let
        rowObjectDecoder : Decode.Decoder Row
        rowObjectDecoder =
          Decode.at [ "row", "columns" ] (Decode.list columnDecoder)
      in Decode.map StreamedRowResponse rowObjectDecoder


    -- Decodes {"properties":...}
    propertiesRespDecoder : Decode.Decoder Response
    propertiesRespDecoder =
      let
        valueDecoder : Decode.Decoder String
        valueDecoder =
          Decode.oneOf
          [ Decode.string
          , Decode.map toString Decode.float
          , Decode.map toString Decode.bool
          ]

        propertiesObjectDecoder : Decode.Decoder (Dict String String)
        propertiesObjectDecoder =
          Decode.at [ "properties", "properties" ] (Decode.dict valueDecoder)
      in Decode.map PropertiesResponse propertiesObjectDecoder


    -- Decodes {"queries":...}
    queriesRespDecoder : Decode.Decoder Response
    queriesRespDecoder =
      let
        queriesObjectDecoder : Decode.Decoder (List Row)
        queriesObjectDecoder =
          let
            entryDecoder : Decode.Decoder Row
            entryDecoder =
              Decode.map3
                (\id -> \kafkaTopic -> \queryString -> [ id, kafkaTopic, queryString ])
                (Decode.at [ "id", "id" ] columnDecoder)
                (Decode.field "kafkaTopic" columnDecoder)
                (Decode.field "queryString" columnDecoder)
          in Decode.at [ "queries", "queries" ] (Decode.list entryDecoder)
      in
        Decode.map
          ( flip TableAndNotificationMessageResponse
            "For detailed information on a Query run: EXPLAIN <Query ID>;"
            << Table [ StringColumn "Query ID", StringColumn "Kafka Topic", StringColumn "Query String" ]
            << List.reverse
          )
          queriesObjectDecoder


    -- Decodes {"streams":...}
    streamsRespDecoder : Decode.Decoder Response
    streamsRespDecoder =
      let
        streamsObjectDecoder : Decode.Decoder (List Row)
        streamsObjectDecoder =
          let
            entryDecoder : Decode.Decoder Row
            entryDecoder =
              Decode.map3
                (\name -> \topic -> \format -> [ name, topic, format ])
                (Decode.field "name" columnDecoder)
                (Decode.field "topic" columnDecoder)
                (Decode.field "format" columnDecoder)
          in Decode.at [ "streams", "streams" ] (Decode.list entryDecoder)
      in
        Decode.map
          ( TableResponse
            << Table
               [ StringColumn "Stream Name"
               , StringColumn "Kafka Topic"
               , StringColumn "Format"
               ]
            << List.reverse
          )
          streamsObjectDecoder


    -- Decodes {"tables":...}
    tablesRespDecoder : Decode.Decoder Response
    tablesRespDecoder =
      let
        tablesObjectDecoder : Decode.Decoder (List Row)
        tablesObjectDecoder =
          let
            entryDecoder : Decode.Decoder Row
            entryDecoder =
              Decode.map4
                (\name -> \topic -> \format -> \windowed -> [ name, topic, format, windowed ])
                (Decode.field "name" columnDecoder)
                (Decode.field "topic" columnDecoder)
                (Decode.field "format" columnDecoder)
                (Decode.field "isWindowed" columnDecoder)
          in Decode.at [ "tables", "tables" ] (Decode.list entryDecoder)
      in
        Decode.map
          ( TableResponse
            << Table
               [ StringColumn "Stream Name"
               , StringColumn "Kafka Topic"
               , StringColumn "Format"
               , StringColumn "Windowed"
               ]
            << List.reverse
          )
          tablesObjectDecoder


    -- Decodes {"kafka_topics":...}
    topicsRespDecoder : Decode.Decoder Response
    topicsRespDecoder =
      let
        topicsObjectDecoder : Decode.Decoder (List Row)
        topicsObjectDecoder =
          let
            entryDecoder : Decode.Decoder Row
            entryDecoder =
              Decode.map6
                (\name -> \registered -> \parts -> \partsReplica -> \consumers -> \consumerGroups ->
                  [ name, registered, parts, partsReplica, consumers, consumerGroups ]
                )
                (Decode.field "name" columnDecoder)
                (Decode.field "registered" columnDecoder)
                (Decode.field "partitionCount" columnDecoder)
                (Decode.field "replicaInfo" columnDecoder)
                (Decode.field "consumerCount" columnDecoder)
                (Decode.field "consumerGroupCount" columnDecoder)
          in Decode.at [ "kafka_topics", "topics" ] (Decode.list entryDecoder)
      in
        Decode.map
          ( TableResponse
            << Table
               [ StringColumn "Kafka Topic"
               , StringColumn "Registered"
               , StringColumn "Partitions"
               , StringColumn "Partition Replicas"
               , StringColumn "Consumers"
               , StringColumn "Consumer Groups"
               ]
            << List.reverse
          )
          topicsObjectDecoder


    -- Decodes {"description":...}
    descrRespDecoder : Decode.Decoder Response
    descrRespDecoder =
      let
        entryDecoder : Decode.Decoder Row
        entryDecoder =
          Decode.map2 (\name -> \typename -> [ name, typename ])
            (Decode.field "name" columnDecoder)
            (Decode.field "type" columnDecoder)


        descrObjectTypeDecoder : Decode.Decoder (String, Bool)
        descrObjectTypeDecoder =
          Decode.map2 (curry identity)
            (Decode.field "type" Decode.string)
            (Decode.field "extended" Decode.bool)


        descrObjectBasicSchemaDecoder : Decode.Decoder (List Row)
        descrObjectBasicSchemaDecoder =
          Decode.field "schema" (Decode.list entryDecoder)


        descrObjectStatsDecoder : Decode.Decoder Statistics
        descrObjectStatsDecoder =
          Decode.map2 Statistics
            (Decode.field "statistics" Decode.string)
            (Decode.field "errorStats" Decode.string)


        descrObjectTopicDecoder : Decode.Decoder Topic
        descrObjectTopicDecoder =
          Decode.map3 Topic
            (Decode.field "kafkaTopic" Decode.string)
            (Decode.field "partitions" Decode.int)
            (Decode.field "replication" Decode.int)


        descrObjectExtendedSchemaDecoder : Decode.Decoder ExtendedSchema
        descrObjectExtendedSchemaDecoder =
          Decode.map8 ExtendedSchema
            (Decode.field "type" Decode.string)
            (Decode.field "key" Decode.string)
            (Decode.field "timestamp" Decode.string)
            (Decode.field "serdes" Decode.string)
            descrObjectTopicDecoder
            (Decode.field "schema" (Decode.list entryDecoder))
            (Decode.field "writeQueries" (Decode.list Decode.string))
            descrObjectStatsDecoder


        descrObjectExplainDecoder : Decode.Decoder ExecutionPlan
        descrObjectExplainDecoder =
          Decode.map5 ExecutionPlan
            (Decode.field "statementText" Decode.string)
            descrObjectStatsDecoder
            descrObjectTopicDecoder
            (Decode.field "executionPlan" Decode.string)
            (Decode.field "topology" Decode.string)
      in
        Decode.field "description"
          (descrObjectTypeDecoder |> Decode.andThen
            ( \(descrType, extended) ->
              case (descrType, extended) of
                ("QUERY", _) ->
                  Decode.map ExplainResponse descrObjectExplainDecoder
                (_, False) -> -- TABLE/STREAM
                  Decode.map
                    ( flip TableAndNotificationMessageResponse
                      "For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;"
                      << Table [ StringColumn "Field", StringColumn "Type" ]
                      << List.reverse
                    )
                    descrObjectBasicSchemaDecoder
                (_, True) -> -- TABLE/STREAM
                  Decode.map DescribeExtendedResponse descrObjectExtendedSchemaDecoder
            )
          )


    -- Decodes {"currentStatus":...}
    curStatusDecoder : Decode.Decoder Response
    curStatusDecoder =
      let
        currentStatusObjectDecoder : Decode.Decoder (Bool, String)
        currentStatusObjectDecoder =
          Decode.map2
            (\status -> \message -> (status == "SUCCESS", message))
            (Decode.at [ "currentStatus", "commandStatus", "status" ] Decode.string)
            (Decode.at [ "currentStatus", "commandStatus", "message" ] Decode.string)
      in
        Decode.map
          ( \(success, message) ->
            if success then NotificationMessageResponse message
            else ErrorMessageResponse message
          )
          currentStatusObjectDecoder


    -- Decodes plain strings (PRINT response)
    rawContentDecoder : Decode.Decoder Response
    rawContentDecoder =
      Decode.map StreamedTextResponse Decode.string


    -- Decodes {"errorMessage":...}
    notificationRespDecoder : Decode.Decoder Response
    notificationRespDecoder =
      let
        notificationObjectDecoder : Decode.Decoder String
        notificationObjectDecoder =
          Decode.at [ "errorMessage", "message" ] Decode.string
      in Decode.map NotificationMessageResponse notificationObjectDecoder


    -- Decodes {"error":...}
    errorMessageRespDecoder : Decode.Decoder Response
    errorMessageRespDecoder =
      let
        errorMessageObjectDecoder : Decode.Decoder String
        errorMessageObjectDecoder =
          Decode.at [ "error", "errorMessage", "message" ] Decode.string
      in Decode.map ErrorMessageResponse errorMessageObjectDecoder
  in
    Decode.oneOf
      [ ksqlWebRespDecoder
      , rowRespDecoder
      , propertiesRespDecoder
      , queriesRespDecoder
      , streamsRespDecoder
      , tablesRespDecoder
      , topicsRespDecoder
      , descrRespDecoder
      , curStatusDecoder
      , rawContentDecoder
      , notificationRespDecoder
      , errorMessageRespDecoder
      ]


escapeSqlString : String -> String
escapeSqlString =
  Regex.replace Regex.All (Regex.regex "'") (\_ -> "''")


unescapeSqlString : String -> String
unescapeSqlString =
  Regex.replace Regex.All (Regex.regex "''") (\_ -> "'")


scrollToBottom : Cmd Msg
scrollToBottom =
  Task.attempt (always NoOp) (Dom.Scroll.toBottom "output")


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    PerformInTimedState cmd state ->
      ( { model | state = state }
      , cmd
      )

    ChangeQuery queryText ->
      let
        query : Query
        query = model.query
      in
        ( { model
          | query = { query | editBuffers = Dict.insert query.currentHistoryIndex queryText query.editBuffers }
          }
        , Cmd.none
        )

    RunQuery ->
      let
        resetModel : Model
        resetModel =
          { model
          | result = Nothing
          , notifications = []
          , errorMessages = []
          }

        queryText : String
        queryText =
          String.trim (currentQueryText model.query)
      in
        if String.isEmpty queryText then (resetModel, Cmd.none)
        else if String.startsWith "SET " (String.toUpper queryText) then
          case List.map .submatches (Regex.find Regex.All setStatementRegex queryText) of
            [ [ Just propEsc, Just valueEsc ] ] ->
              let
                prop : String
                prop = unescapeSqlString propEsc
              in
                ( { resetModel
                  | properties = Dict.insert prop (unescapeSqlString valueEsc) model.properties
                  , notifications =
                    [ "Successfully changed local property '" ++ propEsc ++
                      "' from '" ++ (escapeSqlString (Maybe.withDefault "" (Dict.get prop model.properties))) ++
                      "' to '" ++ valueEsc ++ "'"
                    ]
                  }
                , localStorageGetQueryHistoryCmd ()
                )
            _ ->
              ( { resetModel
                | errorMessages = [ "Syntax error in SET statement, correct syntax is \nSET '...'='...';" ]
                }
              , Cmd.none
              )
        else if String.startsWith "UNSET " (String.toUpper queryText) then
          case List.map .submatches (Regex.find Regex.All unsetStatementRegex queryText) of
            [ [ Just propEsc ] ] ->
              let
                prop : String
                prop = unescapeSqlString propEsc
              in
                ( { resetModel
                  | properties = Dict.remove prop model.properties
                  , notifications =
                    [ "Successfully unset local property '" ++ propEsc ++
                      "' (value was '" ++
                      (escapeSqlString (Maybe.withDefault "" (Dict.get prop model.properties))) ++
                      "')"
                    ]
                  }
                , localStorageGetQueryHistoryCmd ()
                )
            _ ->
              ( { resetModel
                | errorMessages = [ "Syntax error in UNSET statement, correct syntax is:\nUNSET '...';" ]
                }
              , Cmd.none
              )
        else
          ( resetModel
          , Cmd.batch
            [ Task.perform
              ( PerformInTimedState
                (sendQuery model.flags model.properties queryText)
                << Running << DeterminateProgress 0
              )
              Time.now
            , localStorageGetQueryHistoryCmd ()
            ]
          )

    PauseQuery ->
      let
        updatedState : State
        updatedState =
          case model.state of
            Streaming live progress -> Streaming (not live) progress
            other -> other

        updatedCmd : Cmd Msg
        updatedCmd =
          case updatedState of
            Streaming True _ -> scrollToBottom
            _ -> Cmd.none
      in
        case model.result of
          Just (StreamingTabularResult rows) ->
            ( { model
              | state = updatedState
              , result = Just (StreamingTabularResult (Stream.togglePause rows))
              }
            , updatedCmd
            )
          Just (StreamingTextualResult lines) ->
            ( { model
              | state = updatedState
              , result = Just (StreamingTextualResult (Stream.togglePause lines))
              }
            , updatedCmd
            )
          _ -> (model, Cmd.none)

    StopQuery ->
      ( { model | state = Idle }
      , WebSocket.send (webSocketUrl model.flags) """{"cmd":"stop"}"""
      )

    UpdateQueryHistory queryHistory ->
      let
        queryText : String
        queryText = currentQueryText model.query

        updatedQueryHistory : List String
        updatedQueryHistory =
          case queryHistory of
            [] -> [ queryText ]
            firstQueryTextInHistory :: _ ->
              if firstQueryTextInHistory == queryText then queryHistory
              else List.take maxQueryHistoryItems (queryText :: queryHistory)
      in
        ( { model | query = Query updatedQueryHistory 0 Dict.empty }
        , localStorageSetQueryHistoryCmd updatedQueryHistory
        )

    UsePrevQueryInHistory ->
      let
        existingQuery : Query
        existingQuery = model.query

        updatedQuery : Query
        updatedQuery =
          { existingQuery
          | currentHistoryIndex =
            min (List.length existingQuery.queryHistory - 1) (existingQuery.currentHistoryIndex + 1)
          }
      in
        if existingQuery == updatedQuery then
          ( model, Cmd.none)
        else
          ( { model | query = updatedQuery }
          , let
              value : String
              value = currentQueryText updatedQuery
            in
              codeMirrorDocSetValueCursorCmd
              { value = value
              , cursor = {line = List.length (String.lines value), ch = 0} }
          )

    UseNextQueryInHistory ->
      let
        existingQuery : Query
        existingQuery = model.query

        updatedQuery : Query
        updatedQuery =
          { existingQuery
          | currentHistoryIndex = max -1 (existingQuery.currentHistoryIndex - 1)
          }
      in
        if existingQuery == updatedQuery then
          ( model, Cmd.none)
        else
          ( { model | query = updatedQuery }
          , let
              value : String
              value = currentQueryText updatedQuery
            in
              codeMirrorDocSetValueCursorCmd
              { value = value
              , cursor = {line = 0, ch = 1} }
          )

    WebSocketIncoming responseJson ->
      let
        responsesResult : Result String (List Response)
        responsesResult =
          Decode.decodeString (Decode.list responseDecoder) responseJson
      in
        ( case responsesResult of
            Ok responses ->
              List.foldr
                ( \response -> \model ->
                  case response of
                    MetaConnected ->
                      case model.state of
                        Initializing _ -> { model | state = Idle }
                        _ -> model

                    StreamedRowResponse row ->
                      let
                        rows : Stream Row
                        rows =
                          case model.result of
                            Just (StreamingTabularResult rows) -> rows
                            _ -> Stream.empty maxDisplayedRows

                        updatedState : State
                        updatedState =
                          case model.state of
                            Running progress -> Streaming True (IndeterminateProgress [ 0 ] progress.currentTime)
                            other -> other
                      in
                        { model
                        | state = updatedState
                        , result = Just (StreamingTabularResult (row ::: rows))
                        }

                    TableResponse table ->
                      { model
                      | state = Idle
                      , result = Just (TabularResult table)
                      }

                    NotificationMessageResponse msg ->
                      { model
                      | state = Idle
                      ,notifications = msg :: model.notifications
                      }

                    TableAndNotificationMessageResponse table msg ->
                      { model
                      | state = Idle
                      , result = Just (TabularResult table)
                      , notifications = msg :: model.notifications
                      }

                    PropertiesResponse props ->
                      { model
                      | state = Idle
                      , result = Just (PropertiesResult props)
                      }

                    DescribeExtendedResponse extendedSchema ->
                      { model
                      | state = Idle
                      , result = Just (DescribeExtendedResult extendedSchema)
                      }

                    ExplainResponse executionPlan ->
                      { model
                      | state = Idle
                      , result = Just (ExplainResult executionPlan)
                      }

                    MetaRawContentFollows format ->
                      let
                        lines : Stream String
                        lines =
                          case model.result of
                            Just (StreamingTextualResult lines) -> lines
                            _ -> ("Format: " ++ format) ::: Stream.empty maxDisplayedRows
                      in
                        { model
                        | result = Just (StreamingTextualResult lines)
                        }

                    StreamedTextResponse line ->
                      let
                        lines : Stream String
                        lines =
                          case model.result of
                            Just (StreamingTextualResult lines) -> lines
                            _ -> Stream.empty maxDisplayedRows

                        updatedState : State
                        updatedState =
                          case model.state of
                            Running progress -> Streaming True (IndeterminateProgress [ 0 ] progress.currentTime)
                            other -> other
                      in
                        { model
                        | state = updatedState
                        , result = Just (StreamingTextualResult (line ::: lines))
                        }

                    ErrorMessageResponse msg ->
                      let
                        newErrorMessages : List String
                        newErrorMessages =
                          case String.lines msg of
                            errorMessage :: _ -> errorMessage :: model.errorMessages
                            [] -> model.errorMessages
                      in
                        { model
                        | state = Idle
                        , errorMessages = newErrorMessages
                        }
                )
                model
                responses
            Err errorMsg ->
              { model | errorMessages = [ "Error parsing JSON (" ++ errorMsg ++ "):\n" ++ responseJson ] }
        , case (responsesResult, model.state) of
            (Ok [ MetaConnected ], Streaming _ _) ->
              sendQuery model.flags model.properties (currentQueryText model.query)
            (_, Streaming True _) -> scrollToBottom
            _ -> Cmd.none
        )

    SendWebSocketKeepAlive _ ->
      ( model
      , WebSocket.send (webSocketUrl model.flags) "{}"
      )

    ProgressTick time ->
      ( case model.state of
          Initializing progress ->
            let
              updatedProgress : DeterminateProgress
              updatedProgress =
                { progress
                | duration = progress.duration + time - progress.currentTime
                , currentTime = time
                }
            in
              { model | state = Initializing updatedProgress }
          Running progress ->
            let
              updatedProgress : DeterminateProgress
              updatedProgress =
                { progress
                | duration = progress.duration + time - progress.currentTime
                , currentTime = time
                }
            in
              { model | state = Running updatedProgress }
          Streaming live progress ->
            let
              updatedProgress : IndeterminateProgress
              updatedProgress =
                let
                  duration : Time
                  duration = time - progress.currentTime

                  markerDurations : List Time
                  markerDurations =
                    if not live then progress.markerDurations
                    else List.map ((+) duration) progress.markerDurations
                in
                  { progress
                  | markerDurations = markerDurations
                  , currentTime = time
                  }
            in
              { model | state = Streaming live updatedProgress }
          _ -> model
      , case model.state of
          Streaming True _ ->
            Random.generate ProgressAddRandomMarker (Random.int 0 15)
          _ -> Cmd.none
      )

    ProgressAddRandomMarker numMarkers ->
      case model.state of
        Streaming True { markerDurations, currentTime } ->
          let
            newMarkerDurations : List Time
            newMarkerDurations =
              case numMarkers of
                1 -> [ 0 ]
                2 -> [ 66 ]
                3 -> [ 0, 66 ]
                4 -> [ 133 ]
                5 -> [ 0, 133 ]
                6 -> [ 66, 133 ]
                7 -> [ 0, 66, 133 ]
                _ -> []

            updatedMarkerDurations : List Time
            updatedMarkerDurations =
              if List.all (\t -> t > progressMarkerLife) markerDurations then newMarkerDurations
              else newMarkerDurations ++ markerDurations
          in
            ( { model | state = Streaming True (IndeterminateProgress updatedMarkerDurations currentTime) }
            , Cmd.none
            )
        _ -> (model, Cmd.none)

    NoOp -> (model, Cmd.none) -- No-op
