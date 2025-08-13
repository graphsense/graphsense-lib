module Shared exposing
    ( Flags, decoder
    , Model, Msg
    , init, update, subscriptions
    )

{-|

@docs Flags, decoder
@docs Model, Msg
@docs init, update, subscriptions

-}

import Api
import Api.Stats
import Api.Taxonomy
import Effect exposing (Effect)
import Json.Decode
import Route exposing (Route)
import Route.Path
import Shared.Model
import Shared.Msg



-- FLAGS


type alias Flags =
    {}


decoder : Json.Decode.Decoder Flags
decoder =
    Json.Decode.succeed {}



-- INIT


type alias Model =
    Shared.Model.Model


init : Result Json.Decode.Error Flags -> Route () -> ( Model, Effect Msg )
init flagsResult route =
    ( { taxonomy = Api.Loading, stats = Api.Loading }
    , [ Api.Taxonomy.loadTagstoreTaxomomy
            { onResponse = Shared.Msg.TagstoreTaxonomyResponded }
            |> Effect.sendCmd
      , Api.Stats.loadTagstoreStats
            { onResponse = Shared.Msg.TagstoreStatsResponded }
            |> Effect.sendCmd
      ]
        |> Effect.batch
    )



-- UPDATE


type alias Msg =
    Shared.Msg.Msg


update : Route () -> Msg -> Model -> ( Model, Effect Msg )
update route msg model =
    case msg of
        Shared.Msg.TagstoreTaxonomyResponded (Ok r) ->
            ( { model | taxonomy = Api.Success r }, Effect.none )

        Shared.Msg.TagstoreTaxonomyResponded (Err r) ->
            ( { model | taxonomy = Api.Failure r }, Effect.none )

        Shared.Msg.TagstoreStatsResponded (Ok r) ->
            ( { model | stats = Api.Success r }, Effect.none )

        Shared.Msg.TagstoreStatsResponded (Err r) ->
            ( { model | stats = Api.Failure r }, Effect.none )



-- SUBSCRIPTIONS


subscriptions : Route () -> Model -> Sub Msg
subscriptions route model =
    Sub.none
