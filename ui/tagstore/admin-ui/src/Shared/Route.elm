module Shared.Route exposing (SearchType(..), getSearchRoute)

import Dict exposing (Dict)
import Route.Path
import Url


type SearchType
    = Actor
    | Identifier
    | Label


getSearchRoute :
    SearchType
    -> String
    ->
        { path : Route.Path.Path
        , query : Dict String String
        , hash : Maybe String
        }
getSearchRoute st q =
    case st of
        Actor ->
            { path = Route.Path.Tags_Search_Keyword_ { keyword = Url.percentEncode q }, query = Dict.fromList [ ( "type", "actor" ) ], hash = Nothing }

        Identifier ->
            { path = Route.Path.Tags_Search_Keyword_ { keyword = Url.percentEncode q }, query = Dict.fromList [ ( "type", "ident" ) ], hash = Nothing }

        Label ->
            { path = Route.Path.Tags_Search_Keyword_ { keyword = Url.percentEncode q }, query = Dict.fromList [ ( "type", "label" ) ], hash = Nothing }
