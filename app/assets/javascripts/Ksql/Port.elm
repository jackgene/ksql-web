port module Ksql.Port exposing (..)


port localStorageSetQueryHistoryCmd : List String -> Cmd msg
port localStorageGetQueryHistoryCmd : () -> Cmd msg
port localStorageGetQueryHistorySub : (List String -> msg) -> Sub msg
port codeMirrorFromTextAreaCmd : String -> Cmd msg
port codeMirrorDocSetValueCursorCmd : { value : String, cursor : { ch : Int, line : Int } } -> Cmd msg
port codeMirrorDocValueChangedSub : (String -> msg) -> Sub msg
port codeMirrorKeyMapRunQuerySub : (() -> msg) -> Sub msg
port codeMirrorKeyMapPrevInHistorySub : (() -> msg) -> Sub msg
port codeMirrorKeyMapNextInHistorySub : (() -> msg) -> Sub msg
