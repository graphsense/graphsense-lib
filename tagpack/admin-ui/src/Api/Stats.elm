module Api.Stats exposing (Stats, loadTagstoreStats)

import Api exposing (baseUrl)
import Dict
import Http
import Json.Decode


type alias NetworkStat =
    { nr_tags : Int
    , nr_identifiers_explicit : Int
    , nr_identifiers_implicit : Int
    , nr_labels : Int
    }


type alias Stats =
    { by_network : Dict.Dict String NetworkStat
    }


tagstoreNetworkStatsDecoder : Json.Decode.Decoder NetworkStat
tagstoreNetworkStatsDecoder =
    Json.Decode.map4 NetworkStat
        (Json.Decode.field "nr_tags" Json.Decode.int)
        (Json.Decode.field "nr_identifiers_explicit" Json.Decode.int)
        (Json.Decode.field "nr_identifiers_implicit" Json.Decode.int)
        (Json.Decode.field "nr_labels" Json.Decode.int)


tagstoreStatsDecoder : Json.Decode.Decoder Stats
tagstoreStatsDecoder =
    Json.Decode.map Stats
        (Json.Decode.field "by_network" (Json.Decode.dict tagstoreNetworkStatsDecoder))


loadTagstoreStats :
    { onResponse : Result Http.Error Stats -> msg
    }
    -> Cmd msg
loadTagstoreStats options =
    Http.get
        { url = baseUrl ++ "/statistics"
        , expect = Http.expectJson options.onResponse tagstoreStatsDecoder
        }
