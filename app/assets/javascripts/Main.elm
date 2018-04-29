port module Main exposing (..)

import Dom
import Dom.Scroll
import Html exposing (..)
import Html.Attributes exposing (autofocus, class, href, id, target, title)
import Html.Events exposing (onClick)
import Http
import Json.Decode as Decode
import Json.Encode as Encode
import Stream exposing (Stream, (:::))
import Task
import Time exposing (Time, second)
import WebSocket


port localStorageSetItemCmd : (String, String) -> Cmd msg
port codeMirrorFromTextAreaCmd : String -> Cmd msg
port codeMirrorDocSetValueCmd : String -> Cmd msg
port codeMirrorDocValueChangedSub : (String -> msg) -> Sub msg
port codeMirrorKeyMapRunQuerySub : (() -> msg) -> Sub msg
port codeMirrorKeyMapPauseQuerySub : (() -> msg) -> Sub msg
port codeMirrorKeyMapStopQuerySub : (() -> msg) -> Sub msg


-- Model
type alias Flags =
  { secure : Bool
  , host : String
  , search : String
  , initialQuery : String
  }


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
  ( Model flags "" Nothing [] []
  , Cmd.batch
    ( ( case (runOnInit flags.search, queryFromSearch flags.search) of
          (True, Just query) -> [ sendQuery flags query ]
          _ -> []
      ) ++
      [ codeMirrorDocSetValueCmd (Maybe.withDefault flags.initialQuery (queryFromSearch flags.search))
      , codeMirrorFromTextAreaCmd "source"
      ]
    )
  )


-- Update
type Msg
  = ChangeQuery String
  | RunQuery
  | PauseQuery
  | StopQuery
  | WebSocketIncoming String
  | SendWebSocketKeepAlive Time
  | ConsoleScrolled (Result Dom.Error ())


type Response
  -- Meta... responses are KSQL Web specific control messages
  = MetaRawContentFollows String
  -- Everything else come from the KSQL REST API
  | StreamedRowResponse Row
  | StreamedTextResponse String
  | TableResponse Table
  | DescribeExtendedResponse ExtendedSchema
  | ExplainResponse ExecutionPlan
  | NotificationMessageResponse String
  | TableAndNotificationMessageResponse Table String
  | ErrorMessageResponse String


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
              _ ->
                Decode.map MetaRawContentFollows (Decode.field "format" Decode.string)
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


maxDisplayedRows : Int
maxDisplayedRows = 5000


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
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
      , sendQuery model.flags model.query
      )
    PauseQuery ->
      case model.result of
        Just (StreamingTabularResult rows) ->
          ( { model | result = Just (StreamingTabularResult (Stream.togglePause rows)) }
          , if Stream.isPaused rows then
              Task.attempt ConsoleScrolled (Dom.Scroll.toBottom "output")
            else Cmd.none
          )
        _ ->
          ( model, Cmd.none )
    StopQuery ->
      ( model
      , WebSocket.send (webSocketUrl model.flags) """{"cmd":"stop"}"""
      )
    WebSocketIncoming responseJson ->
      ( case Decode.decodeString (Decode.list responseDecoder) responseJson of
          Ok responses ->
            List.foldr
              ( \response -> \model ->
                case response of
                  StreamedRowResponse row ->
                    let
                      rows : Stream Row
                      rows =
                        case model.result of
                          Just (StreamingTabularResult rows) -> rows
                          _ -> Stream.empty maxDisplayedRows
                    in
                      { model | result = Just (StreamingTabularResult (row ::: rows)) }

                  TableResponse table ->
                    { model | result = Just (TabularResult table) }

                  NotificationMessageResponse msg ->
                    { model | notifications = msg :: model.notifications }

                  TableAndNotificationMessageResponse table msg ->
                    { model
                    | result = Just (TabularResult table)
                    , notifications = msg :: model.notifications
                    }

                  DescribeExtendedResponse extendedSchema ->
                    { model | result = Just (DescribeExtendedResult extendedSchema) }

                  ExplainResponse executionPlan ->
                    { model | result = Just (ExplainResult executionPlan) }

                  MetaRawContentFollows format ->
                    { model
                    | result = Just (StreamingTextualResult (("Format: " ++ format) ::: Stream.empty maxDisplayedRows))
                    }

                  StreamedTextResponse line ->
                    let
                      lines : Stream String
                      lines =
                        case model.result of
                          Just (StreamingTextualResult lines) -> lines
                          _ -> Stream.empty maxDisplayedRows
                    in
                      { model | result = Just (StreamingTextualResult (line ::: lines)) }

                  ErrorMessageResponse msg ->
                    let
                      newErrorMessages : List String
                      newErrorMessages =
                        case String.lines msg of
                          errorMessage :: _ -> errorMessage :: model.errorMessages
                          [] -> model.errorMessages
                    in
                      { model | errorMessages = newErrorMessages }
              )
              model
              responses
          Err errorMsg ->
            { model | errorMessages = [ "Error parsing JSON:\n" ++ responseJson ] }
      , case model.result of
          Just (StreamingTabularResult rows) ->
            if (Stream.isPaused rows) then Cmd.none
            else Task.attempt ConsoleScrolled (Dom.Scroll.toBottom "output")
          Just (StreamingTextualResult lines) ->
            if (Stream.isPaused lines) then Cmd.none
            else Task.attempt ConsoleScrolled (Dom.Scroll.toBottom "output")
          _ -> Cmd.none
      )
    SendWebSocketKeepAlive _ ->
      ( model
      , WebSocket.send (webSocketUrl model.flags) "{}"
      )
    ConsoleScrolled _ ->
      ( model, Cmd.none ) -- No-op


-- Subscription
subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.batch
    [ codeMirrorDocValueChangedSub ChangeQuery
    , codeMirrorKeyMapRunQuerySub (always RunQuery)
    , codeMirrorKeyMapPauseQuerySub (always PauseQuery)
    , codeMirrorKeyMapStopQuerySub (always StopQuery)
    , Time.every (60 * second) SendWebSocketKeepAlive
    , WebSocket.listen (webSocketUrl model.flags) WebSocketIncoming
    ]


-- View
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
            [ onClick RunQuery, title "Run Query (Shift-Enter)" ]
            [ text "▶" ]
          , button
            [ onClick PauseQuery, title "Pause/Resume Query (Ctrl-P)" ]
            [ text "️❙❙" ]
          , button
            [ onClick StopQuery, title "Stop Query (Ctrl-C)" ]
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
      [ textarea [ id "source", autofocus True ] [ text model.query ] ]
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
              messagesView Nothing (List.reverse (Stream.items lines))

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
