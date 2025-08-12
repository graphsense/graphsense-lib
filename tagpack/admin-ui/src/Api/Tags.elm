module Api.Tags exposing (Tag, TagQuery, TagsQueryConfig(..), loadTagstoreTags)

import Api exposing (baseUrl)
import Dict
import Http
import Json.Decode exposing (bool, field, int, list, maybe, string)
import Shared.Paging exposing (PagingState)


type TagsQueryConfig
    = QueryByActor String
    | QueryByLabel String
    | QueryByIdentifier String


type alias TagQuery =
    { network : Maybe String
    , query : TagsQueryConfig
    }


type alias Tag =
    { identifier : String
    , label : String
    , source : Maybe String
    , creator : Maybe String
    , confidence : Maybe String
    , confidence_level : Maybe Int
    , tag_subject : String
    , tag_type : String
    , actor : Maybe String
    , primary_concept : Maybe String
    , additional_concepts : List String
    , is_cluster_definer : Bool
    , network : String
    , last_mod : Int
    , group : String
    , inherited_from : Maybe String
    , tagpack_uri : String
    , tagpack_title : String
    }


tagstoreTagDecoder : Json.Decode.Decoder Tag
tagstoreTagDecoder =
    let
        mainFields =
            Json.Decode.map6 Tag
                (field "identifier" string)
                (field "label" string)
                (field "source" (maybe string))
                (field "creator" (maybe string))
                (field "confidence" (maybe string))
                (field "confidence_level" (maybe int))

        conceptsAndActor =
            Json.Decode.map6
                (<|)
                mainFields
                (field "tag_subject" string)
                (field "tag_type" string)
                (field "actor" (maybe string))
                (field "primary_concept" (maybe string))
                (field "additional_concepts" (list string))
    in
    Json.Decode.map8
        (<|)
        conceptsAndActor
        (field "is_cluster_definer" bool)
        (field "network" string)
        (field "lastmod" int)
        (field "group" string)
        (field "inherited_from" (maybe string))
        (field "tagpack_uri" string)
        (field "tagpack_title" string)


tagstoreTagsDecoder : Json.Decode.Decoder (List Tag)
tagstoreTagsDecoder =
    Json.Decode.list tagstoreTagDecoder


loadTagstoreTags :
    { onResponse : PagingState -> Result Http.Error (List Tag) -> msg
    , query : TagQuery
    , page : PagingState
    }
    -> Cmd msg
loadTagstoreTags options =
    let
        lp =
            options.page

        ps =
            options.page.pageSize

        page_config =
            { lp | pageSize = ps }

        filterFragment =
            case options.query.query of
                QueryByActor a ->
                    "actor_id=" ++ a

                QueryByLabel l ->
                    "label=" ++ l

                QueryByIdentifier i ->
                    "subject_id=" ++ i

        networkFilter =
            options.query.network |> Maybe.map (\z -> "&network=" ++ z) |> Maybe.withDefault ""
    in
    Http.get
        { url = baseUrl ++ "/tags?" ++ filterFragment ++ "&page_nr=" ++ String.fromInt page_config.page ++ "&page_size=" ++ String.fromInt ps ++ "&groups=public&groups=private" ++ networkFilter
        , expect = Http.expectJson (options.onResponse page_config) tagstoreTagsDecoder
        }
