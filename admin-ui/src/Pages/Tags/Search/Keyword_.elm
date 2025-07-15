module Pages.Tags.Search.Keyword_ exposing (Model, Msg, page)

import Api
import Api.Tags exposing (Tag, loadTagstoreTags)
import Components.TagsTable
import Dict exposing (Dict)
import Effect exposing (Effect)
import Html
import Http
import Page exposing (Page)
import Route exposing (Route)
import Shared
import Shared.Model exposing (TagsPage)
import Shared.Paging exposing (PagingState)
import View exposing (View)


page : Shared.Model -> Route { keyword : String } -> Page Model Msg
page shared route =
    let
        query =
            { network = route.query |> Dict.get "network" |> Maybe.map String.toUpper
            , query =
                case route.query |> Dict.get "type" of
                    Just "actor" ->
                        Api.Tags.QueryByActor route.params.keyword

                    Just "ident" ->
                        Api.Tags.QueryByIdentifier route.params.keyword

                    _ ->
                        Api.Tags.QueryByLabel route.params.keyword
            }

        pageNr =
            route.query |> Dict.get "page" |> Maybe.andThen String.toInt

        pageSize =
            route.query |> Dict.get "pageSize" |> Maybe.andThen String.toInt |> Maybe.withDefault 15
    in
    Page.new
        { init = init query pageNr pageSize
        , update = update query
        , subscriptions = subscriptions
        , view = view query
        }



-- INIT


type alias Model =
    { tagsTable : Components.TagsTable.Model
    }


loadPage : Api.Tags.TagQuery -> PagingState -> (PagingState -> Result Http.Error (List Tag) -> Msg) -> Effect Msg
loadPage query pageS message =
    loadTagstoreTags
        { onResponse = message
        , query = query
        , page = pageS
        }
        |> Effect.sendCmd


init : Api.Tags.TagQuery -> Maybe Int -> Int -> () -> ( Model, Effect Msg )
init q pageNr pageSize () =
    Components.TagsTable.init
        { loadPage = loadPage q
        , toMsg = TagsTableMsg
        , toModel = \x -> { tagsTable = x }
        , pageSize = pageSize
        , page = pageNr
        }



-- UPDATE


type Msg
    = TagsTableMsg Components.TagsTable.Msg


update : Api.Tags.TagQuery -> Msg -> Model -> ( Model, Effect Msg )
update query msg model =
    case msg of
        TagsTableMsg inner ->
            Components.TagsTable.update
                { msg = inner
                , model = model.tagsTable
                , toModel = \x -> { model | tagsTable = x }
                , toMsg = TagsTableMsg
                , loadPage = loadPage query
                }



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none



-- VIEW


view : Api.Tags.TagQuery -> Model -> View Msg
view _ model =
    { title = "Tags Search Results"
    , body =
        [ Components.TagsTable.new
            { model = model.tagsTable
            , toMsg = TagsTableMsg
            }
            |> Components.TagsTable.view
        ]
    }
