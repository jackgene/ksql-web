module Stream exposing
  ( Stream
  , empty
  , (:::)
  , items
  , isPaused
  , togglePause
  )


type Stream a
  = Live Int (List a)
  | Paused Int (List a) (List a)


empty : Int -> Stream a
empty limit =
  Live limit []


addToStream : Int -> a -> List a -> List a
addToStream limit item items =
  List.take limit (item :: items)


(:::) : a -> Stream a -> Stream a
(:::) item stream =
  case stream of
    Live limit items ->
      Live limit (addToStream limit item items)
    Paused limit items buffer ->
      Paused limit items (addToStream limit item buffer)


infixr 5 :::


items : Stream a -> List a
items stream =
  case stream of
    Live _ items -> items
    Paused _ items _ -> items


isPaused : Stream a -> Bool
isPaused stream =
  case stream of
    Live _ _ -> False
    Paused _ _ _ -> True


togglePause : Stream a -> Stream a
togglePause stream =
  case stream of
    Live limit items ->
      Paused limit items []
    Paused limit items buffer ->
      Live limit (List.take limit (buffer ++ items))
