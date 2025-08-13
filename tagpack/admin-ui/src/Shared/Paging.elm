module Shared.Paging exposing (PagingState)


type alias PagingState =
    { page : Int
    , pageSize : Int
    }
