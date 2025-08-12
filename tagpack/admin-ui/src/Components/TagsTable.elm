module Components.TagsTable exposing (Model, Msg, TagsTable, init, new, update, view)

import Api
import Api.Tags exposing (Tag)
import Effect exposing (Effect)
import Html exposing (Html, a, button, div, i, li, nav, span, table, tbody, td, text, th, thead, tr, ul)
import Html.Attributes exposing (class, disabled, href, title)
import Html.Events exposing (onClick)
import Http
import Route
import Shared.Paging exposing (PagingState)
import Shared.Route exposing (SearchType(..), getSearchRoute)


toParentModel : (Model -> model) -> ( Model, Effect msg ) -> ( model, Effect msg )
toParentModel toModel ( innerModel, effect ) =
    ( toModel innerModel
    , effect
    )


type TagsTable msg
    = Settings
        { model : Model
        , toMsg : Msg -> msg
        }


new :
    { toMsg : Msg -> msg
    , model : Model
    }
    -> TagsTable msg
new props =
    Settings
        { toMsg = props.toMsg
        , model = props.model
        }


type Column
    = Label
    | Network
    | Concepts
    | ActorC
    | Ident
    | Tagpack
    | IsClusterDef
    | Confidence


type Model
    = Model
        { tags : Api.Data (List Tag)
        , page : PagingState
        , columns : List Column
        }


hasMorePages : Model -> Bool
hasMorePages (Model m) =
    case m.tags of
        Api.Success t ->
            List.length t == m.page.pageSize

        _ ->
            False


hasPreviousPage : Model -> Bool
hasPreviousPage (Model m) =
    case m.tags of
        Api.Success t ->
            m.page.page > 0

        _ ->
            False



--- INIT


init :
    { loadPage : PagingState -> (PagingState -> Result Http.Error (List Tag) -> msg) -> Effect msg
    , toMsg : Msg -> msg
    , toModel : Model -> model
    , pageSize : Int
    , page : Maybe Int
    }
    -> ( model, Effect msg )
init props =
    let
        ip =
            { page = props.page |> Maybe.withDefault 0, pageSize = props.pageSize }
    in
    ( Model
        { tags = Api.Loading
        , page = ip
        , columns = [ Ident, Label, Network, Concepts, ActorC, Tagpack, IsClusterDef, Confidence ]
        }
    , props.loadPage ip (\a b -> GotData a b |> props.toMsg)
    )
        |> toParentModel props.toModel



--- UPDATE


type Msg
    = NoOp
    | GotData PagingState (Result Http.Error (List Tag))
    | UserClickedNextPage
    | UserClickedPreviousPage


update :
    { msg : Msg
    , model : Model
    , toModel : Model -> model
    , toMsg : Msg -> msg
    , loadPage : PagingState -> (PagingState -> Result Http.Error (List Tag) -> msg) -> Effect msg
    }
    -> ( model, Effect msg )
update props =
    let
        (Model model) =
            props.model
    in
    toParentModel props.toModel <|
        case props.msg of
            NoOp ->
                ( Model model
                , Effect.none
                )

            GotData ps (Ok t) ->
                ( Model { model | tags = Api.Success t, page = ps }
                , Effect.none
                )

            GotData _ (Err t) ->
                ( Model { model | tags = Api.Failure t }
                , Effect.none
                )

            UserClickedNextPage ->
                let
                    page =
                        model.page

                    nextPage =
                        { page | page = page.page + 1 }
                in
                ( Model { model | tags = Api.Loading }
                , props.loadPage nextPage (\a b -> GotData a b |> props.toMsg)
                )

            UserClickedPreviousPage ->
                let
                    page =
                        model.page

                    nextPage =
                        { page | page = page.page - 1 }
                in
                ( Model { model | tags = Api.Loading }
                , props.loadPage nextPage (\a b -> GotData a b |> props.toMsg)
                )



--- VIEW


columnToName : Column -> String
columnToName c =
    case c of
        Label ->
            "Label"

        Network ->
            "Network"

        Concepts ->
            "Concepts"

        ActorC ->
            "Actor"

        Ident ->
            "Ident"

        Tagpack ->
            "Pack"

        IsClusterDef ->
            "Clsr d.?"

        Confidence ->
            "Confidence"


tag : List String -> String -> Html msg
tag attr x =
    span [ class ("tag " ++ String.join " " attr) ] [ text x ]


renderColumnCell : Tag -> Column -> Html msg
renderColumnCell t c =
    case c of
        Network ->
            tag [ "is-black" ] t.network

        Label ->
            text t.label

        Concepts ->
            let
                primary =
                    t.primary_concept
                        |> Maybe.map (tag [ "is-primary" ] >> List.singleton)
                        |> Maybe.withDefault []

                sec =
                    t.additional_concepts |> List.map (tag [ "is-light" ])
            in
            div [ class "tags" ] (primary ++ sec)

        ActorC ->
            case t.actor of
                Just atr ->
                    a [ Route.href (getSearchRoute Actor atr) ] [ text atr ]

                _ ->
                    text "-"

        Ident ->
            let
                ident =
                    t.identifier

                textPart =
                    if String.length ident > 42 then
                        String.left 42 ident ++ "..."

                    else
                        ident
            in
            a [ Route.href (getSearchRoute Identifier ident) ] [ text textPart ]

        Tagpack ->
            a [ href t.tagpack_uri ] [ text t.tagpack_title ]

        IsClusterDef ->
            span [ class "icon" ]
                [ i
                    [ class
                        (if t.is_cluster_definer then
                            "fas fa-check-square has-text-success"

                         else
                            "fas fa-ban has-text-danger"
                        )
                    ]
                    []
                ]

        Confidence ->
            let
                cl =
                    t.confidence_level |> Maybe.withDefault 0

                ct =
                    (t.confidence |> Maybe.withDefault "-") ++ " (" ++ String.fromInt cl ++ ")"

                clr =
                    if cl > 60 then
                        "has-text-success"

                    else if cl > 30 then
                        "has-text-warning"

                    else
                        "has-text-danger"
            in
            span [ class "icon", title ct ] [ i [ class ("fas fa-solid fa-circle " ++ clr) ] [] ]


view : TagsTable msg -> Html msg
view (Settings settings) =
    let
        (Model model) =
            settings.model

        renderCell t c =
            td [] [ renderColumnCell t c ]

        renderRow cs t =
            tr [] (cs |> List.map (renderCell t))

        thcell c =
            th [] [ text (columnToName c) ]
    in
    div [ class "block" ]
        [ table [ class "table is-striped is-hoverable is-fullwidth" ]
            [ thead []
                [ tr [] (model.columns |> List.map thcell)
                ]
            , tbody []
                (case model.tags of
                    Api.Success tags ->
                        tags |> List.map (renderRow model.columns)

                    Api.Loading ->
                        [ text "Looking for data..." ]

                    _ ->
                        [ text "No tags found" ]
                )
            ]
        , nav [ class "pagination is-centered" ]
            [ button [ class "pagination-previous", onClick (UserClickedPreviousPage |> settings.toMsg), disabled (not (hasPreviousPage settings.model)) ] [ text "previous" ]
            , button [ class "pagination-next", onClick (UserClickedNextPage |> settings.toMsg), disabled (not (hasMorePages settings.model)) ] [ text "next" ]
            , ul [ class "pagination-list" ]
                [ li [] [ div [ class "pagination-link is-current" ] [ text (String.fromInt (model.page.page + 1)) ] ]
                ]
            ]
        ]
