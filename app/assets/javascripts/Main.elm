port module Main exposing (..)

import Dom.Scroll
import Html exposing (..)
import Html.Attributes exposing (autofocus, class, href, id, src, style, target, title)
import Html.Events exposing (onClick)
import Http
import Json.Decode as Decode
import Json.Encode as Encode
import Keyboard
import Random
import Stream exposing (Stream, (:::))
import Task
import Time exposing (Time, millisecond, second)
import WebSocket


port localStorageSetItemCmd : (String, String) -> Cmd msg
port codeMirrorFromTextAreaCmd : String -> Cmd msg
port codeMirrorDocSetValueCmd : String -> Cmd msg
port codeMirrorDocValueChangedSub : (String -> msg) -> Sub msg
port codeMirrorKeyMapRunQuerySub : (() -> msg) -> Sub msg


maxDisplayedRows : Int
maxDisplayedRows = 5000


progressMarkerLife : Time
progressMarkerLife = 1.5 * second


progressTickPeriod : Time
progressTickPeriod = 200 * millisecond


-- Model
type alias Flags =
  { secure : Bool
  , host : String
  , search : String
  , initialQuery : String
  }


type alias DeterminateProgress =
  { duration : Time
  , currentTime : Time
  }


type alias IndeterminateProgress =
  { markerDurations : List Time
  , currentTime : Time
  }


type State
  = Initializing DeterminateProgress
  | Idle
  | Running DeterminateProgress
  | Streaming Bool IndeterminateProgress


type Column
  = BoolColumn Bool
  | IntColumn Int
  | StringColumn String
  | NullColumn
  | ArrayColumn (List Column)


type alias Row = List Column


-- Tabular data, result of most "SHOW ..."/"LIST ..."/"DESCRIBE ..." operations
type alias Table =
  { headerRow : Row
  , dataRows : List Row
  }


type alias Statistics =
  { statistics : String
  , errorStats : String
  }


type alias Topic =
  { name : String
  , partitions : Int
  , replication : Int
  }


-- Result of "DESCRIBE EXTENDED ..."
type alias ExtendedSchema =
  { schemaType : String
  , key : String
  , timestamp : String
  , serdes : String
  , kafkaOutputTopic : Topic
  , schema : List Row
  , writeQueries : List String
  , statistics : Statistics
  }


-- Result of "EXPLAIN ..."
type alias ExecutionPlan =
  { statementText : String
  , statistics : Statistics
  , kafkaOutputTopic : Topic
  , executionPlan : String
  , topology : String
  }


type QueryResult
  = StreamingTabularResult (Stream Row)
  | StreamingTextualResult (Stream String)
  | TabularResult Table
  | DescribeExtendedResult ExtendedSchema
  | ExplainResult ExecutionPlan


type alias Model =
  { flags : Flags
  , state : State
  , query : String
  , result : Maybe QueryResult
  , notifications : List String
  , errorMessages : List String
  }


webSocketUrl : Flags -> String
webSocketUrl flag =
  (if flag.secure then "wss" else "ws") ++ "://" ++ flag.host ++ "/ksql"


ksqlCommandJson : String -> Encode.Value
ksqlCommandJson query =
  Encode.object [ ("ksql", Encode.string query) ]


sendQuery : Flags -> String -> Cmd msg
sendQuery flags query =
  WebSocket.send (webSocketUrl flags) (Encode.encode 0 (ksqlCommandJson query))


searchParts : String -> List String
searchParts search =
  (String.split "&" (String.dropLeft 1 search))


queryFromSearch : String -> Maybe String
queryFromSearch search =
  List.head
    ( List.filterMap
      (\searchPart ->
        case String.split "=" searchPart of
          [ "query", query ] -> Http.decodeUri query
          _ -> Nothing
      )
      (searchParts search)
    )


runOnInit  : String -> Bool
runOnInit search =
  not (List.isEmpty (List.filter (String.startsWith "run") (searchParts search)))


init : Flags -> (Model, Cmd Msg)
init flags =
  ( Model flags Idle "" Nothing [] []
  , Cmd.batch
    [ Task.perform
        ( PerformInTimedState
          ( case (runOnInit flags.search, queryFromSearch flags.search) of
              (True, Just query) -> sendQuery flags query
              _ -> Cmd.none
          )
          << Initializing << DeterminateProgress 0
        )
        Time.now
    , codeMirrorDocSetValueCmd (Maybe.withDefault flags.initialQuery (queryFromSearch flags.search))
    , codeMirrorFromTextAreaCmd "source"
    ]
  )


-- Update
type Msg
  = PerformInTimedState (Cmd Msg) State
  | ChangeQuery String
  | RunQuery
  | PauseQuery
  | StopQuery
  | WebSocketIncoming String
  | SendWebSocketKeepAlive Time
  | ProgressTick Time
  | ProgressAddRandomMarker Int
  | NoOp


-- JSON response from WebSocket
type Response
  -- Meta... responses are KSQL Web specific control messages
  = MetaConnected
  | MetaRawContentFollows String
  -- Everything else come from the KSQL REST API
  | StreamedRowResponse Row
  | StreamedTextResponse String
  | TableResponse Table
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

        intColumnDecoder : Decode.Decoder Column
        intColumnDecoder = Decode.map IntColumn Decode.int

        stringColumnDecoder : Decode.Decoder Column
        stringColumnDecoder = Decode.map StringColumn Decode.string

        nullColumnDecoder : Decode.Decoder Column
        nullColumnDecoder = Decode.null NullColumn

        arrayColumnDecoder : Decode.Decoder Column
        arrayColumnDecoder = Decode.map ArrayColumn (Decode.list (Decode.lazy (\_ -> columnDecoder)))
      in
        Decode.oneOf
        [ boolColumnDecoder, intColumnDecoder, stringColumnDecoder, nullColumnDecoder, arrayColumnDecoder ]


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
        propertiesObjectDecoder : Decode.Decoder (List Row)
        propertiesObjectDecoder =
          Decode.map
            (\kvPairs -> List.map (\(k, v) -> [ StringColumn k, v ]) kvPairs)
            (Decode.at [ "properties", "properties" ] (Decode.keyValuePairs columnDecoder))
      in
        Decode.map
          (TableResponse << Table [ StringColumn "Property", StringColumn "Value" ])
          propertiesObjectDecoder


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

    ChangeQuery query ->
      ( { model | query = query }
      , localStorageSetItemCmd ("query", query)
      )

    RunQuery ->
      ( { model
        | result = Nothing
        , notifications = []
        , errorMessages = []
        }
      , Task.perform
        ( PerformInTimedState
          (sendQuery model.flags model.query)
          << Running << DeterminateProgress 0
        )
        Time.now
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
              sendQuery model.flags model.query
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
          Streaming True progress ->
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


-- Subscription
subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.batch
    [ codeMirrorDocValueChangedSub ChangeQuery
    , codeMirrorKeyMapRunQuerySub (always RunQuery)
    , Keyboard.presses
      ( \code -> case code of
        -- Ctrl+C
        3 -> StopQuery
        -- Ctrl+Z
        26 -> PauseQuery
        _ -> NoOp
      )
    , Time.every (60 * second) SendWebSocketKeepAlive
    , WebSocket.listen (webSocketUrl model.flags) WebSocketIncoming
    , case model.state of
        Idle -> Sub.none
        _ -> Time.every progressTickPeriod ProgressTick
    ]


-- View
progressBarView : Float -> Html Msg
progressBarView completion =
  div
    [ id "bar", style [ ("width", (toString (completion * 100) ++ "%")) ] ]
    []


rowKeyType : List Row -> Maybe String
rowKeyType schema =
  List.head
    ( List.filterMap
      ( \row ->
        case row of
        [ StringColumn "ROWKEY", StringColumn "VARCHAR(STRING)" ] -> Just "STRING"
        [ StringColumn "ROWKEY", StringColumn keyType ] -> Just keyType
        _ -> Nothing
      )
      schema
    )


colContentView : Column -> List (Html Msg)
colContentView col =
  case col of
    BoolColumn value ->
      [ text (String.toLower (toString value)) ]
    IntColumn value ->
      [ text (toString value) ]
    StringColumn value ->
      [ text value ]
    NullColumn ->
      [ span [ class "meta" ] [ text "(null)" ] ]
    ArrayColumn values ->
      List.intersperse
        (span [ class "meta" ] [ text ", " ])
        (List.concatMap colContentView values)


colView : Bool -> Column -> Html Msg
colView isHeader col =
  (if isHeader then th else td)
  []
  (colContentView col)

rowView : Bool -> Row -> Html Msg
rowView isHeader row =
  tr [] (List.map (colView isHeader) row)


metadataRowView : String -> String -> Html Msg
metadataRowView key value =
  tr []
    [ th [] [ text key ]
    , td [] [ text ":" ]
    , td [] [ text value ]
    ]


metadataTableView : List (String,String) -> Html Msg
metadataTableView metadata =
  table [ class "metadata" ]
    (List.map (uncurry metadataRowView) metadata)


messagesView : Maybe String -> List String -> List (Html Msg)
messagesView maybeClassName messages =
  case messages of
    [] -> []
    nonEmptyMsgs ->
      [ div
        ( class "messages" ::
          ( case maybeClassName of
              Just className -> [ class className ]
              Nothing -> []
          )
        )
        ( List.map
          (\msg -> pre [] [ text msg ])
          nonEmptyMsgs
        )
      ]


view : Model -> Html Msg
view model =
  div []
    [ div [ id "control" ]
      ( [ div [ class "primary" ]
          [ button
            [ onClick RunQuery, title "Run Query (Shift+Enter)" ]
            [ text "▶" ]
          , button
            [ onClick PauseQuery, title "Pause/Resume Query (Ctrl+Shift+Z)" ]
            [ text "️❙❙" ]
          , button
            [ onClick StopQuery, title "Stop Query (Ctrl+Shift+C)" ]
            [ text "◼" ]
          ]
        ] ++
        ( if String.isEmpty model.query then []
          else
            [ div [ class "primary" ]
              [ a [ href ("?query=" ++ (Http.encodeUri model.query)) ]
                [ text "Link to this Query" ]
              ]
            ]
        ) ++
        [ div [ class "secondary" ]
          [ a
            [ href "https://docs.confluent.io/current/ksql/docs/syntax-reference.html"
            , target "_blank"
            ]
            [ text "KSQL Syntax Reference" ]
          ]
        ]
      )
    , div [ id "input" ]
      [ div [] [ textarea [ id "source", autofocus True ] [ text model.query ] ]
      , div [ id "progress" ]
        [ case model.state of
            Initializing progress -> progressBarView ((progress.duration / (1 * second)) ^ 2)
            Running progress -> progressBarView ((progress.duration / (600 * second)) ^ 0.02)
            Streaming live progress ->
              let
                extraBarStyles : List (String, String)
                extraBarStyles =
                  if live || (floor progress.currentTime) // (floor second) % 2 == 0 then []
                  else [ ("background", "#fff") ]
              in
                div [ id "bar", style (("width", "100%") :: extraBarStyles ) ]
                ( List.foldl
                  ( \marker -> \gapViews ->
                    ( div
                      [ class "gap"
                      , style
                        [ ("left", (toString ((marker / progressMarkerLife) ^ 2 * 100)) ++ "%")
                        , ("width", (toString ((marker / progressMarkerLife) ^ 2 * 40)) ++ "px") ]
                      ]
                      []
                    ) :: gapViews
                  )
                  []
                  progress.markerDurations
                )
            Idle -> progressBarView 1.0
        ]
      ]
    , div [ id "output" ]
      ( ( case model.result of
            Just (StreamingTabularResult rows) ->
              [ table [ class "data" ]
                ( List.foldl
                  (\row -> \rowViews -> (rowView False row) :: rowViews)
                  []
                  (Stream.items rows)
                )
              ]

            Just (TabularResult {headerRow, dataRows}) ->
              [ table [ class "data" ]
                ( rowView True headerRow ::
                  ( List.foldl
                    (\row -> \rowViews -> (rowView False row) :: rowViews)
                    []
                    dataRows
                  )
                )
              ]

            Just (DescribeExtendedResult schema) ->
              ( metadataTableView
                [ ( "Type", schema.schemaType )
                , ( "Key field", schema.key )
                , ( "Timestamp field"
                  , ( if String.isEmpty schema.timestamp then "Not set - using <ROWTIME>"
                      else schema.timestamp
                    )
                  )
                , ( "Key format", Maybe.withDefault "" (rowKeyType schema.schema) )
                , ( "Value format", schema.serdes )
                , ( "Kafka output topic"
                  , ( schema.kafkaOutputTopic.name
                    ++ " (partitions: " ++ (toString schema.kafkaOutputTopic.partitions)
                    ++ ", replication: " ++ (toString schema.kafkaOutputTopic.replication)
                    ++ ")"
                    )
                  )
                ]
              ) ::
              (br [] []) ::
              ( table [ class "data" ]
                ( rowView True [ StringColumn "Field", StringColumn "Type" ] ::
                  ( List.map (rowView False) schema.schema )
                )
              ) ::
              ( if List.isEmpty schema.writeQueries then []
                else
                  (h3 [] [ text ("Queries that write into this " ++ schema.schemaType) ]) ::
                  (messagesView Nothing schema.writeQueries)
              ) ++
              [ p [] [ text "For query topology and execution plan please run: EXPLAIN <QueryId>" ] ] ++
              [ h3 [] [ text "Local runtime statistics" ] ] ++
              messagesView Nothing [ schema.statistics.statistics ] ++
              messagesView Nothing [ schema.statistics.errorStats ] ++
              [ p []
                [ text ("(Statistics of the local KSQL server interaction with the Kafka topic " ++ schema.kafkaOutputTopic.name ++ ")") ]
              ]

            Just (ExplainResult plan) ->
              ( metadataTableView
                ( ("Type", "QUERY") ::
                  if (String.isEmpty plan.statementText) then []
                  else [ ("SQL", plan.statementText) ]
                )
              ) ::
              [ h3 [] [ text "Local runtime statistics" ] ] ++
              messagesView Nothing [ plan.statistics.statistics ] ++
              messagesView Nothing [ plan.statistics.errorStats ] ++
              [ p []
                [ text ("(Statistics of the local KSQL server interaction with the Kafka topic " ++ plan.kafkaOutputTopic.name ++ ")") ]
              ] ++
              [ h3 [] [ text "Execution plan" ] ] ++
              messagesView Nothing [ plan.executionPlan ] ++
              [ h3 [] [ text "Processing topology" ] ] ++
              messagesView Nothing [ plan.topology ]

            Just (StreamingTextualResult lines) ->
              [ div [ class "messages" ]
                ( List.foldl
                  (\line -> \lineViews -> pre [] [ text line ] :: lineViews)
                  []
                  (Stream.items lines)
                )
              ]

            Nothing -> []
        ) ++
        messagesView Nothing model.notifications ++
        messagesView (Just "error") model.errorMessages
      )
    ]


main : Program Flags Model Msg
main =
  Html.programWithFlags
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }
