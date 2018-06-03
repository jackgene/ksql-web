import Html
import Ksql.Common exposing (Flags, Model, Msg)
import Ksql.Init exposing (init)
import Ksql.Subscriptions exposing (subscriptions)
import Ksql.Update exposing (update)
import Ksql.View exposing (view)


main : Program Flags Model Msg
main =
  Html.programWithFlags
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }
