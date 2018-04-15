port module Main exposing (..)

import Dom
import Dom.Scroll
import Html exposing (..)
import Html.Attributes exposing (autofocus, class, id)
import Html.Events exposing (onClick)
import Json.Decode as Decode
import Json.Encode as Encode
import Task
import Time exposing (Time, second)
import WebSocket


maxDisplayedRows : Int
maxDisplayedRows = 10000


webSocketUrl : Request -> String
webSocketUrl request =
  (if request.secure then "wss" else "ws") ++ "://" ++ request.host ++ "/ksql"


port codeMirrorFromTextAreaCmd : String -> Cmd msg
port codeMirrorDocSetValueCmd : String -> Cmd msg
port codeMirrorDocValueChangedSub : (String -> msg) -> Sub msg


-- Model
type alias Request =
  { secure : Bool
  , host : String
  }
type Column
  = BoolColumn Bool
  | IntColumn Int
  | StringColumn String
  | NullColumn
type alias Row = List Column
type alias QueryResult =
  { headerRow : Maybe Row
  , dataRows : List Row
  }
type alias Model =
  { request : Request
  , query : String
  , result : QueryResult
  , maybeBufferedDataRows : Maybe (List Row)
  , notifications : List String
  , errorMessages : List String
  }


init : Request -> (Model, Cmd Msg)
init request =
  ( Model request "" (QueryResult Nothing []) Nothing [] []
  , codeMirrorFromTextAreaCmd "source"
  )


-- Update
type Msg
  = ChangeQuery String
  | RunQuery
  | PauseQuery
  | StopQuery
  | QueryResponse String
  | SendWebSocketKeepAlive Time
  | ConsoleScrolled (Result Dom.Error ())


type Response
  = RowResponse Row
  | ShowPropertiesResponse (List Row)
  | ShowQueriesResponse (List Row)
  | ShowStreamsResponse (List Row)
  | ShowTablesResponse (List Row)
  | ShowTopicsResponse (List Row)
  | DescribeResponse (List Row)
  | NotificationResponse String
  | ErrorMessageResponse String


ksqlCommandJson : String -> Encode.Value
ksqlCommandJson query =
  Encode.object [ ("ksql", Encode.string query) ]


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
  in
    Decode.oneOf [ boolColumnDecoder, intColumnDecoder, stringColumnDecoder, nullColumnDecoder ]


rowObjectDecoder : Decode.Decoder Row
rowObjectDecoder =
  let
    rowDecoder : Decode.Decoder Row
    rowDecoder = Decode.list columnDecoder
  in
    Decode.at [ "row", "columns" ] rowDecoder


propertiesObjectDecoder : Decode.Decoder (List Row)
propertiesObjectDecoder =
  Decode.map
    (\kvPairs -> List.map (\(k, v) -> [ StringColumn k, v ]) kvPairs)
    (Decode.at [ "properties", "properties" ] (Decode.keyValuePairs columnDecoder))


queriesObjectDecoder : Decode.Decoder (List Row)
queriesObjectDecoder =
  let
    entryDecoder : Decode.Decoder Row
    entryDecoder =
      Decode.map3
        (\id -> \kafkaTopic -> \queryString -> [ id, kafkaTopic, queryString ])
        (Decode.at [ "id", "id" ] columnDecoder)
        (Decode.field "queryString" columnDecoder)
        (Decode.field "kafkaTopic" columnDecoder)
  in
    Decode.at [ "queries", "queries" ] (Decode.list entryDecoder)


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
  in
    Decode.at [ "streams", "streams" ] (Decode.list entryDecoder)


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
  in
    Decode.at [ "tables", "tables" ] (Decode.list entryDecoder)


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
  in
    Decode.at [ "kafka_topics", "topics" ] (Decode.list entryDecoder)


descriptionObjectDecoder : Decode.Decoder ((List Row), String)
descriptionObjectDecoder =
  let
    entryDecoder : Decode.Decoder Row
    entryDecoder =
      Decode.map2
        (\name -> \typename -> [ name, typename ])
        (Decode.field "name" columnDecoder)
        (Decode.field "type" columnDecoder)
  in
    Decode.map2
      (\schema -> \executionPlan -> (schema, executionPlan))
      (Decode.at [ "description", "schema" ] (Decode.list entryDecoder))
      (Decode.at [ "description", "executionPlan" ] Decode.string)


currentStatusObjectDecoder : Decode.Decoder (Bool, String)
currentStatusObjectDecoder =
  Decode.map2
    (\status -> \message -> (status == "SUCCESS", message))
    (Decode.at [ "currentStatus", "commandStatus", "status" ] Decode.string)
    (Decode.at [ "currentStatus", "commandStatus", "message" ] Decode.string)


notificationObjectDecoder : Decode.Decoder String
notificationObjectDecoder =
  Decode.at [ "errorMessage", "message" ] Decode.string


errorMessageObjectDecoder : Decode.Decoder String
errorMessageObjectDecoder =
  Decode.at [ "error", "errorMessage", "message" ] Decode.string


responseDecoder : Decode.Decoder Response
responseDecoder =
  let
    rowRespDecoder : Decode.Decoder Response
    rowRespDecoder =
      Decode.map RowResponse rowObjectDecoder

    propertiesRespDecoder : Decode.Decoder Response
    propertiesRespDecoder =
      Decode.map ShowPropertiesResponse propertiesObjectDecoder

    queriesRespDecoder : Decode.Decoder Response
    queriesRespDecoder =
      Decode.map ShowQueriesResponse queriesObjectDecoder

    streamsRespDecoder : Decode.Decoder Response
    streamsRespDecoder =
      Decode.map ShowStreamsResponse streamsObjectDecoder

    tablesRespDecoder : Decode.Decoder Response
    tablesRespDecoder =
      Decode.map ShowTablesResponse tablesObjectDecoder

    topicsRespDecoder : Decode.Decoder Response
    topicsRespDecoder =
      Decode.map ShowTopicsResponse topicsObjectDecoder

    descrRespDecoder : Decode.Decoder Response
    descrRespDecoder =
      Decode.map
        ( \(schema, executionPlan) ->
          if not (List.isEmpty schema) then DescribeResponse schema
          else if not (String.isEmpty executionPlan) then NotificationResponse executionPlan
          else ErrorMessageResponse "Description response has neither schema nor executionPlan."
        )
        descriptionObjectDecoder

    curStatusDecoder : Decode.Decoder Response
    curStatusDecoder =
      Decode.map
        ( \(success, message) ->
          if success then NotificationResponse message
          else ErrorMessageResponse message
        )
        currentStatusObjectDecoder

    notificationRespDecoder : Decode.Decoder Response
    notificationRespDecoder =
      Decode.map NotificationResponse notificationObjectDecoder

    errorMessageRespDecoder : Decode.Decoder Response
    errorMessageRespDecoder =
      Decode.map ErrorMessageResponse errorMessageObjectDecoder
  in
    Decode.oneOf
      [ rowRespDecoder
      , propertiesRespDecoder
      , queriesRespDecoder
      , streamsRespDecoder
      , tablesRespDecoder
      , topicsRespDecoder
      , descrRespDecoder
      , curStatusDecoder
      , notificationRespDecoder
      , errorMessageRespDecoder
      ]


displayedDataRows : List Row -> List Row
displayedDataRows dataRows =
  List.take maxDisplayedRows dataRows


unpauseQuery : List Row -> Model -> Model
unpauseQuery bufferedDataRows model =
  let
    result : QueryResult
    result = model.result
  in
    { model
    | result = { result | dataRows = displayedDataRows (result.dataRows ++ bufferedDataRows) }
    , maybeBufferedDataRows = Nothing
    }


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    ChangeQuery query ->
      ( { model | query = query }
      , Cmd.none
      )
    RunQuery ->
      ( { model
        | result = QueryResult Nothing []
        , maybeBufferedDataRows = Nothing
        , notifications = []
        , errorMessages = []
        }
      , WebSocket.send (webSocketUrl model.request) (Encode.encode 0 (ksqlCommandJson model.query))
      )
    PauseQuery ->
      ( case model.maybeBufferedDataRows of
          Just bufferedDataRows ->
            unpauseQuery bufferedDataRows model
          Nothing ->
            { model | maybeBufferedDataRows = Just [] }
      , Cmd.none )
    StopQuery ->
      ( case model.maybeBufferedDataRows of
          Just bufferedDataRows ->
            unpauseQuery bufferedDataRows model
          Nothing ->
            model
      , WebSocket.send (webSocketUrl model.request) """{"cmd":"stop"}"""
      )
    QueryResponse responseJson ->
      ( case Decode.decodeString (Decode.list responseDecoder) responseJson of
          Ok responses ->
            List.foldr
              ( \response -> \model ->
                case (response, model.maybeBufferedDataRows) of
                  (RowResponse row, Nothing) ->
                    let
                      result : QueryResult
                      result = model.result
                    in
                      { model | result = { result | dataRows = displayedDataRows (row :: model.result.dataRows) } }
                  (RowResponse row, Just bufferedDataRows) ->
                    { model | maybeBufferedDataRows = Just (row :: bufferedDataRows) }
                  (ShowPropertiesResponse properties, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "Property", StringColumn "Value" ])
                        properties
                    }
                  (ShowQueriesResponse queries, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "Query ID", StringColumn "Kafka Topic", StringColumn "Query String" ])
                        queries
                    }
                  (ShowStreamsResponse streams, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "Stream Name", StringColumn "Kafka Topic", StringColumn "Format" ])
                        streams
                    }
                  (ShowTablesResponse tables, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "Stream Name", StringColumn "Kafka Topic", StringColumn "Format", StringColumn "Windowed" ])
                        tables
                    }
                  (ShowTopicsResponse topics, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "Kafka Topic", StringColumn "Registered", StringColumn "Partitions", StringColumn "Partition Replicas", StringColumn "Consumers", StringColumn "Consumer Groups" ])
                        topics
                    }
                  (DescribeResponse metaRows, _) ->
                    { model
                    | result
                      = QueryResult
                        ( Just [ StringColumn "name", StringColumn "type" ])
                        metaRows
                    }
                  (NotificationResponse msg, _) ->
                    { model | notifications = msg :: model.notifications }
                  (ErrorMessageResponse msg, _) ->
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
      , case model.maybeBufferedDataRows of
          Just _ -> Cmd.none
          Nothing -> Task.attempt ConsoleScrolled (Dom.Scroll.toBottom "output")
      )
    SendWebSocketKeepAlive _ ->
      ( model
      , WebSocket.send (webSocketUrl model.request) "{}"
      )
    ConsoleScrolled _ ->
      ( model, Cmd.none ) -- No-op


-- Subscription
subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.batch
    [ codeMirrorDocValueChangedSub ChangeQuery
    , Time.every (60 * second) SendWebSocketKeepAlive
    , WebSocket.listen (webSocketUrl model.request) QueryResponse
    ]


-- View
colView : Bool -> Column -> Html Msg
colView isHeader col =
  (if isHeader then th else td)
  []
  [ case col of
      BoolColumn value ->
        text (String.toLower (toString value))
      IntColumn value ->
        text (toString value)
      StringColumn value ->
        text value
      NullColumn ->
        span [ class "null" ] [ text "(null)" ]
  ]

rowView : Bool -> Row -> Html Msg
rowView isHeader row =
  tr [] (List.map (colView isHeader) row)


messagesView : Maybe String -> List String -> Html Msg
messagesView maybeClassName messages =
  div
    ( class "messages" ::
      ( case maybeClassName of
          Just className -> [ class className ]
          Nothing -> []
      )
    )
    ( List.map
      (\msg -> pre [] [ text msg ])
      messages
    )


view : Model -> Html Msg
view model =
  div []
  [ div [ id "control" ]
    [ button
      [ onClick RunQuery ]
      [ text "▶" ]
    , button
      [ onClick PauseQuery ]
      [ text "️❙❙" ]
    , button
      [ onClick StopQuery ]
      [ text "◼" ]
    ]
  , div [ id "input" ]
    [ textarea [ id "source", autofocus True ] [ text model.query ] ]
  , div [ id "output" ]
    [ table []
      ( ( case model.result.headerRow of
            Just row -> [ rowView True row ]
            Nothing -> []
        ) ++
        ( List.foldl
          (\row -> \rowViews -> (rowView False row) :: rowViews)
          []
          model.result.dataRows
        )
      )
    , messagesView Nothing model.notifications
    , messagesView (Just "error") model.errorMessages
    ]
  ]


main : Program Request Model Msg
main =
  Html.programWithFlags
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }
