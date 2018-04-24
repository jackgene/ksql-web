port module Main exposing (..)

import Dom
import Dom.Scroll
import Html exposing (..)
import Html.Attributes exposing (autofocus, class, href, id, target)
import Html.Events exposing (onClick)
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


-- Model
type alias Flags =
  { secure : Bool
  , host : String
  , initialQuery : String
  }


type Column
  = BoolColumn Bool
  | IntColumn Int
  | StringColumn String
  | NullColumn


type alias Row = List Column


type alias Table =
  { headerRow : Row
  , dataRows : List Row
  }


type QueryResult
  = StreamingTabularResult (Stream Row)
--  | StreamingTextualResult Stream String
  | TabularResult Row (List Row)
--  | DescribeExtendedResult String Table String


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


init : Flags -> (Model, Cmd Msg)
init flags =
  ( Model flags "" Nothing [] []
  , Cmd.batch
    [ codeMirrorDocSetValueCmd flags.initialQuery
    , codeMirrorFromTextAreaCmd "source"
    ]
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



responseDecoder : Decode.Decoder Response
responseDecoder =
  let
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


    rowRespDecoder : Decode.Decoder Response
    rowRespDecoder =
      let
        rowObjectDecoder : Decode.Decoder Row
        rowObjectDecoder =
          let
            rowDecoder : Decode.Decoder Row
            rowDecoder = Decode.list columnDecoder
          in Decode.at [ "row", "columns" ] rowDecoder
      in Decode.map RowResponse rowObjectDecoder


    propertiesRespDecoder : Decode.Decoder Response
    propertiesRespDecoder =
      let
        propertiesObjectDecoder : Decode.Decoder (List Row)
        propertiesObjectDecoder =
          Decode.map
            (\kvPairs -> List.map (\(k, v) -> [ StringColumn k, v ]) kvPairs)
            (Decode.at [ "properties", "properties" ] (Decode.keyValuePairs columnDecoder))
      in Decode.map ShowPropertiesResponse propertiesObjectDecoder


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
      in Decode.map ShowQueriesResponse queriesObjectDecoder


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
      in Decode.map ShowStreamsResponse streamsObjectDecoder


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
      in Decode.map ShowTablesResponse tablesObjectDecoder


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
      in Decode.map ShowTopicsResponse topicsObjectDecoder


    descrRespDecoder : Decode.Decoder Response
    descrRespDecoder =
      let
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
      in
        Decode.map
          ( \(schema, executionPlan) ->
            if not (List.isEmpty schema) then DescribeResponse schema
            else if not (String.isEmpty executionPlan) then NotificationResponse executionPlan
            else ErrorMessageResponse "Description response has neither schema nor executionPlan."
          )
          descriptionObjectDecoder


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
            if success then NotificationResponse message
            else ErrorMessageResponse message
          )
          currentStatusObjectDecoder


    notificationRespDecoder : Decode.Decoder Response
    notificationRespDecoder =
      let
        notificationObjectDecoder : Decode.Decoder String
        notificationObjectDecoder =
          Decode.at [ "errorMessage", "message" ] Decode.string
      in Decode.map NotificationResponse notificationObjectDecoder


    errorMessageRespDecoder : Decode.Decoder Response
    errorMessageRespDecoder =
      let
        errorMessageObjectDecoder : Decode.Decoder String
        errorMessageObjectDecoder =
          Decode.at [ "error", "errorMessage", "message" ] Decode.string
      in Decode.map ErrorMessageResponse errorMessageObjectDecoder
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
      , WebSocket.send (webSocketUrl model.flags) (Encode.encode 0 (ksqlCommandJson model.query))
      )
    PauseQuery ->
      case model.result of
        Just (StreamingTabularResult rows) ->
          ( { model | result = Just (StreamingTabularResult (Stream.togglePause rows)) }
          , Task.attempt ConsoleScrolled (Dom.Scroll.toBottom "output")
          )
        _ ->
          ( model, Cmd.none )
    StopQuery ->
      ( model
      , WebSocket.send (webSocketUrl model.flags) """{"cmd":"stop"}"""
      )
    QueryResponse responseJson ->
      ( case Decode.decodeString (Decode.list responseDecoder) responseJson of
          Ok responses ->
            List.foldr
              ( \response -> \model ->
                case response of
                  RowResponse row ->
                    let
                      rows : Stream Row
                      rows =
                        case model.result of
                          Just (StreamingTabularResult rows) -> rows
                          _ -> Stream.empty maxDisplayedRows
                    in
                      { model | result = Just (StreamingTabularResult (row ::: rows)) }
                  ShowPropertiesResponse properties ->
                    { model
                    | result = Just (TabularResult [ StringColumn "Property", StringColumn "Value" ] properties)
                    }
                  ShowQueriesResponse queries ->
                    { model
                    | result = Just (TabularResult [ StringColumn "Query ID", StringColumn "Kafka Topic", StringColumn "Query String" ] (List.reverse queries))
                    }
                  ShowStreamsResponse streams ->
                    { model
                    | result = Just (TabularResult [ StringColumn "Stream Name", StringColumn "Kafka Topic", StringColumn "Format" ] (List.reverse streams))
                    }
                  ShowTablesResponse tables ->
                    { model
                    | result = Just (TabularResult [ StringColumn "Stream Name", StringColumn "Kafka Topic", StringColumn "Format", StringColumn "Windowed" ] (List.reverse tables))
                    }
                  ShowTopicsResponse topics ->
                    { model
                    | result = Just (TabularResult [ StringColumn "Kafka Topic", StringColumn "Registered", StringColumn "Partitions", StringColumn "Partition Replicas", StringColumn "Consumers", StringColumn "Consumer Groups" ] (List.reverse topics))
                    }
                  DescribeResponse metaRows ->
                    { model
                    | result = Just (TabularResult [ StringColumn "name", StringColumn "type" ] (List.reverse metaRows))
                    }
                  NotificationResponse msg ->
                    { model | notifications = msg :: model.notifications }
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
    , Time.every (60 * second) SendWebSocketKeepAlive
    , WebSocket.listen (webSocketUrl model.flags) QueryResponse
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
    [ button
      [ onClick RunQuery ]
      [ text "▶" ]
    , button
      [ onClick PauseQuery ]
      [ text "️❙❙" ]
    , button
      [ onClick StopQuery ]
      [ text "◼" ]
    , div []
      [ a
        [ href "https://docs.confluent.io/current/ksql/docs/syntax-reference.html"
        , target "_blank"
        ]
        [ text "KSQL Syntax Reference" ]
      ]
    ]
  , div [ id "input" ]
    [ textarea [ id "source", autofocus True ] [ text model.query ] ]
  , div [ id "output" ]
    ( ( case model.result of
          Just (StreamingTabularResult rows) ->
            [ table []
              ( List.foldl
                (\row -> \rowViews -> (rowView False row) :: rowViews)
                []
                (Stream.items rows)
              )
            ]
          Just (TabularResult headerRow dataRows) ->
            [ table []
              ( rowView True headerRow ::
                ( List.foldl
                  (\row -> \rowViews -> (rowView False row) :: rowViews)
                  []
                  dataRows
                )
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
