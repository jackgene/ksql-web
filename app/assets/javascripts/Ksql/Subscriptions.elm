module Ksql.Subscriptions exposing (subscriptions)

import Keyboard
import Ksql.Common exposing (..)
import Ksql.Port exposing (..)
import Time exposing (Time, millisecond, second)
import WebSocket


progressTickPeriod : Time
progressTickPeriod = 200 * millisecond


-- Subscription
subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.batch
    [ localStorageGetQueryHistorySub UpdateQueryHistory
    , codeMirrorDocValueChangedSub ChangeQuery
    , codeMirrorKeyMapRunQuerySub (always RunQuery)
    , codeMirrorKeyMapPrevInHistorySub (always UsePrevQueryInHistory)
    , codeMirrorKeyMapNextInHistorySub (always UseNextQueryInHistory)
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
