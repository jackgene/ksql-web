module Ksql.View exposing (view)

import Html exposing (..)
import Html.Attributes exposing (autofocus, class, href, id, src, style, target, title)
import Html.Events exposing (onClick)
import Http
import Ksql.Common exposing (..)
import Stream
import Time exposing (second)


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


jsonValueView : Column -> List (Html Msg)
jsonValueView col =
  case col of
    StringColumn value ->
      [ span [ class "meta" ] [ text "\"" ]
      , text value
      , span [ class "meta" ] [ text "\"" ]
      ]
    NullColumn ->
      [ span [ class "meta" ] [ text "null" ] ]
    ArrayColumn values ->
      [ span [ class "meta" ] [ text "[" ] ] ++
      ( List.intersperse
        (span [ class "meta" ] [ text ", " ])
        (List.concatMap jsonValueView values)
      ) ++
      [ span [ class "meta" ] [ text "]" ] ]
    other ->
      colContentView other


colContentView : Column -> List (Html Msg)
colContentView col =
  case col of
    BoolColumn value ->
      [ text (String.toLower (toString value)) ]
    NumericColumn value ->
      [ text (toString value) ]
    StringColumn value ->
      [ text value ]
    NullColumn ->
      [ span [ class "meta" ] [ text "(null)" ] ]
    ArrayColumn values ->
      List.intersperse
        (span [ class "meta" ] [ text ", " ])
        (List.concatMap colContentView values)
    JsonColumn keyValues ->
      [ span [ class "meta" ] [ text "{" ] ] ++
      ( List.intersperse
        (span [ class "meta" ] [ text ", " ])
        ( List.concatMap
          ( \(key, value) ->
            span [ class "meta" ] [ text ("\"" ++ key ++ "\":") ] ::
            jsonValueView value
          )
          keyValues
        )
      ) ++
      [ span [ class "meta" ] [ text "}" ] ]


colView : Bool -> Column -> Html Msg
colView isHeader col =
  (if isHeader then th else td)
  ( case col of
      NumericColumn _ -> [ class "numeric" ]
      _ -> [ class "non-numeric" ]
  )
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
        ( case currentQueryText model.query of
            "" -> []
            nonEmptyQueryText ->
              [ div [ class "primary" ]
                [ a [ href ("?query=" ++ (Http.encodeUri nonEmptyQueryText)) ]
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
      [ div [] [ textarea [ id "source", autofocus True ] [] ]
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
