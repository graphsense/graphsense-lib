module Shared.Msg exposing (Msg(..))

import Api.Stats
import Api.Taxonomy
import Http


{-| Normally, this value would live in "Shared.elm"
but that would lead to a circular dependency import cycle.

For that reason, both `Shared.Model` and `Shared.Msg` are in their
own file, so they can be imported by `Effect.elm`

-}
type Msg
    = TagstoreTaxonomyResponded (Result Http.Error Api.Taxonomy.Taxonomy)
    | TagstoreStatsResponded (Result Http.Error Api.Stats.Stats)
