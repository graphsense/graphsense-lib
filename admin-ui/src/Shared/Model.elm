module Shared.Model exposing (Model, TagsPage)

import Api
import Api.Stats
import Api.Tags exposing (Tag)
import Api.Taxonomy
import Shared.Paging exposing (PagingState)


{-| Normally, this value would live in "Shared.elm"
but that would lead to a circular dependency import cycle.

For that reason, both `Shared.Model` and `Shared.Msg` are in their
own file, so they can be imported by `Effect.elm`

-}
type alias Model =
    { taxonomy : Api.Data Api.Taxonomy.Taxonomy
    , stats : Api.Data Api.Stats.Stats
    }


type alias TagsPage =
    { page : PagingState
    , tags : List Tag
    }
