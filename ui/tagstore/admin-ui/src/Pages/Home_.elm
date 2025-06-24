module Pages.Home_ exposing (Model, Msg, page)

import Api
import Api.Stats
import Components.PieChart
import Dict exposing (Dict)
import Effect exposing (Effect)
import Html exposing (Html, a, button, div, h1, h2, input, label, p, span, text)
import Html.Attributes exposing (checked, class, disabled, name, placeholder, type_)
import Html.Events exposing (onClick, onInput)
import Http
import Page exposing (Page)
import Route exposing (Route)
import Route.Path
import Shared
import Shared.Route exposing (SearchType(..), getSearchRoute)
import View exposing (View)


page : Shared.Model -> Route () -> Page Model Msg
page sm r =
    Page.new
        { init = init
        , update = update
        , subscriptions = subscriptions
        , view = view sm
        }



-- INIT


type alias Model =
    { search_term : String
    , search_type : SearchType
    }


init : () -> ( Model, Effect Msg )
init _ =
    ( { search_term = "", search_type = Label }
    , Effect.none
    )



-- UPDATE


type Msg
    = ChangeSearchType SearchType
    | ChangeSearchTerm String
    | Search


update : Msg -> Model -> ( Model, Effect Msg )
update msg model =
    case msg of
        ChangeSearchType t ->
            ( { model | search_type = t }
            , Effect.none
            )

        ChangeSearchTerm s ->
            ( { model | search_term = s }
            , Effect.none
            )

        search ->
            ( model, Effect.pushRoute (getSearchRoute model.search_type model.search_term) )



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none



-- VIEW


view : Shared.Model -> Model -> View Msg
view sm model =
    { title = "Tagstore Admin UI"
    , body =
        [ div [ class "container" ]
            [ h1 [ class "title" ] [ text "Welcome" ]
            , h2 [ class "subtitle" ] [ text "to the Tagstore Admin UI" ]
            , div [ class "box block" ]
                [ div [ class "control " ]
                    [ input [ class "input", type_ "text", placeholder "search", onInput ChangeSearchTerm ] []
                    ]
                , div [ class "radios block" ]
                    [ label [ class "radio" ]
                        [ input [ type_ "radio", name "search_type", onClick (ChangeSearchType Identifier), checked (model.search_type == Identifier) ] []
                        , text "Address/Tx"
                        ]
                    , label [ class "radio" ]
                        [ input [ type_ "radio", name "search_type", onClick (ChangeSearchType Actor), checked (model.search_type == Actor) ] []
                        , text "Actor"
                        ]
                    , label [ class "radio" ]
                        [ input [ type_ "radio", name "search_type", onClick (ChangeSearchType Label), checked (model.search_type == Label) ] []
                        , text "Label"
                        ]
                    ]
                , div [ class "buttons" ] [ button [ class "button is-primary", onClick Search, type_ "submit", disabled (model.search_term |> String.isEmpty) ] [ text "search" ] ]
                ]
            , div [ class "box block" ]
                (case sm.stats of
                    Api.Loading ->
                        [ text "Loading..." ]

                    Api.Failure _ ->
                        [ text "There was an error fetching data..." ]

                    Api.Success data ->
                        let
                            dataSorted =
                                Dict.toList data.by_network
                                    |> List.map (Tuple.mapSecond (.nr_tags >> toFloat))
                                    |> List.sortWith (\a b -> compare (Tuple.second a) (Tuple.second b))
                                    |> List.reverse

                            head =
                                List.take 5 dataSorted

                            tail =
                                List.drop 5 dataSorted

                            chartData =
                                head ++ [ ( "other", tail |> List.map Tuple.second |> List.sum ) ]
                        in
                        [ p [ class "title is-5 is-spaced" ] [ text "Composition by Currency" ], Components.PieChart.view chartData ]
                )
            ]
        ]
    }
