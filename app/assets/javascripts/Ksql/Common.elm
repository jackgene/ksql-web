module Ksql.Common exposing (..)

import Dict exposing (Dict)
import Json.Encode as Encode
import Stream exposing (Stream)
import Time exposing (Time, second)
import WebSocket


progressMarkerLife : Time
progressMarkerLife = 1.5 * second


webSocketUrl : Flags -> String
webSocketUrl flag =
  (if flag.secure then "wss" else "ws") ++ "://" ++ flag.host ++ "/ksql"


currentQueryText : Query -> String
currentQueryText query =
  Maybe.withDefault "" <| List.head <| List.filterMap
    identity
    [ Dict.get query.currentHistoryIndex query.editBuffers
    , if query.currentHistoryIndex < 0 then Nothing
      else List.head (List.drop query.currentHistoryIndex query.queryHistory)
    ]


ksqlCommandJson : Dict String String -> String -> Encode.Value
ksqlCommandJson props query =
  Encode.object
  [ ("ksql"
    , Encode.string query
    )
  , ("streamsProperties"
    , Encode.object (Dict.toList (Dict.map (\_ -> \value -> Encode.string value) props))
    )
  ]


sendQuery : Flags -> Dict String String -> String -> Cmd msg
sendQuery flags props query =
  WebSocket.send (webSocketUrl flags) (Encode.encode 0 (ksqlCommandJson props query))


-- Model
type alias Flags =
  { secure : Bool
  , host : String
  , search : String
  , queryHistory : List String
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


type alias Query =
  { queryHistory : List String
  , currentHistoryIndex : Int
  , editBuffers : Dict Int String
  }

type Column
  = BoolColumn Bool
  | NumericColumn Float
  | StringColumn String
  | NullColumn
  | ArrayColumn (List Column)
  | JsonColumn (List (String, Column))


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
  | PropertiesResult (Dict String String)
  | DescribeExtendedResult ExtendedSchema
  | ExplainResult ExecutionPlan


type alias Model =
  { flags : Flags
  , state : State
  , properties : Dict String String
  , query : Query
  , result : Maybe QueryResult
  , notifications : List String
  , errorMessages : List String
  }


-- Message
type Msg
  = PerformInTimedState (Cmd Msg) State
  | ChangeQuery String
  | RunQuery
  | PauseQuery
  | StopQuery
  | UpdateQueryHistory (List String)
  | UsePrevQueryInHistory
  | UseNextQueryInHistory
  | WebSocketIncoming String
  | SendWebSocketKeepAlive Time
  | ProgressTick Time
  | ProgressAddRandomMarker Int
  | NoOp
