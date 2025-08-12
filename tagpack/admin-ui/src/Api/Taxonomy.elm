module Api.Taxonomy exposing (Taxonomy, loadTagstoreTaxomomy)

import Api exposing (baseUrl)
import Dict
import Http
import Json.Decode


type alias TaxItem =
    { id : String
    , label : String
    , description : String
    }


type alias Taxonomy =
    { confidence : List TaxItem
    , country : List TaxItem
    , tag_subject : List TaxItem
    , tag_type : List TaxItem
    , concept : List TaxItem
    }


tagstoreTaxItemDecoder : Json.Decode.Decoder TaxItem
tagstoreTaxItemDecoder =
    Json.Decode.map3 TaxItem
        (Json.Decode.field "id" Json.Decode.string)
        (Json.Decode.field "label" Json.Decode.string)
        (Json.Decode.field "description" Json.Decode.string)


tagstoreTaxonomyDecoder : Json.Decode.Decoder Taxonomy
tagstoreTaxonomyDecoder =
    Json.Decode.map5 Taxonomy
        (Json.Decode.field "confidence" (Json.Decode.list tagstoreTaxItemDecoder))
        (Json.Decode.field "country" (Json.Decode.list tagstoreTaxItemDecoder))
        (Json.Decode.field "tag_subject" (Json.Decode.list tagstoreTaxItemDecoder))
        (Json.Decode.field "tag_type" (Json.Decode.list tagstoreTaxItemDecoder))
        (Json.Decode.field "concept" (Json.Decode.list tagstoreTaxItemDecoder))


loadTagstoreTaxomomy :
    { onResponse : Result Http.Error Taxonomy -> msg
    }
    -> Cmd msg
loadTagstoreTaxomomy options =
    Http.get
        { url = baseUrl ++ "/taxonomy"
        , expect = Http.expectJson options.onResponse tagstoreTaxonomyDecoder
        }
