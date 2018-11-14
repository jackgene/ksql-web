module Ksql.Init exposing (init)

import Dict exposing (Dict)
import Http
import Ksql.Common exposing (..)
import Ksql.Port exposing (codeMirrorDocSetValueCursorCmd, codeMirrorFromTextAreaCmd)
import Task
import Time


initProperties : Dict String String
initProperties =
  Dict.fromList [ ("application.id", "ksql-web") ]


searchParts : String -> List String
searchParts search =
  (String.split "&" (String.dropLeft 1 search))


queryTextFromSearch : String -> Maybe String
queryTextFromSearch search =
  List.head
  ( List.filterMap
    (\searchPart ->
      case String.split "=" searchPart of
        [ "query", query ] -> Http.decodeUri query
        _ -> Nothing
    )
    (searchParts search)
  )


runOnInit : String -> Bool
runOnInit search =
  not (List.isEmpty (List.filter (String.startsWith "run") (searchParts search)))


init : Flags -> (Model, Cmd Msg)
init flags =
  ( Model flags Idle initProperties (Query flags.queryHistory -1 (Dict.singleton -1 "")) Nothing [] []
  , Cmd.batch
    [ Task.perform
        ( case (runOnInit flags.search, queryTextFromSearch flags.search) of
            (True, Just queryText) ->
              PerformInTimedState (sendQuery flags Dict.empty queryText) << Running << DeterminateProgress 0
            _ ->
              PerformInTimedState Cmd.none << Initializing << DeterminateProgress 0
        )
        Time.now
    , let
        value : String
        value =
          Maybe.withDefault "" (queryTextFromSearch flags.search)
      in
        codeMirrorDocSetValueCursorCmd
        { value = value, cursor = {line = List.length (String.lines value), ch = 0} }
    , codeMirrorFromTextAreaCmd "source"
    ]
  )
