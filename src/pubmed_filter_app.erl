%%%-------------------------------------------------------------------
%%% @doc PubMed biomedical literature search agent.
%%%
%%% Uses the NCBI E-utilities API (free, no key required) with a
%%% two-step approach: esearch for IDs, then esummary for metadata.
%%%
%%% Deduplication by URL is handled upstream by the Emquest pipeline.
%%%
%%% === Capability cascade ===
%%%
%%%   base_capabilities/0 extends em_filter:base_capabilities().
%%%
%%% Handler contract: handle/2 (Body, Memory) -> {RawList, Memory}.
%%% @end
%%%-------------------------------------------------------------------
-module(pubmed_filter_app).
-behaviour(application).

-export([start/2, stop/1]).
-export([handle/2, base_capabilities/0]).

-define(ESEARCH_URL,
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    "?db=pubmed&retmax=10&retmode=json&term=").
-define(ESUMMARY_URL,
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    "?db=pubmed&retmode=json&id=").

%%====================================================================
%% Capability cascade
%%====================================================================

-spec base_capabilities() -> [binary()].
base_capabilities() ->
    em_filter:base_capabilities() ++ [<<"pubmed">>, <<"medicine">>,
                                      <<"biology">>, <<"research">>,
                                      <<"papers">>, <<"ncbi">>].

%%====================================================================
%% Application lifecycle
%%====================================================================

start(_Type, _Args) ->
    case pubmed_filter_sup:start_link() of
        {ok, Pid} ->
            ok = start_pop_and_http(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    catch cowboy:stop_listener(pubmed_filter_query_listener),
    catch em_pop_sup:stop_node(pubmed_filter),
    ok.

%%====================================================================
%% Internal
%%====================================================================

start_pop_and_http() ->
    PopPort   = application:get_env(pubmed_filter, pop_port,   9486),
    QueryPort = application:get_env(pubmed_filter, query_port, 9487),
    Seeds     = application:get_env(pubmed_filter, pop_seeds,  []),
    Vec = em_filter_vec:from_capabilities(base_capabilities()),
    catch em_pop_sup:stop_node(pubmed_filter),
    catch cowboy:stop_listener(pubmed_filter_query_listener),
    {ok, PopPid} = em_pop_sup:start_node(pubmed_filter, #{
        port            => PopPort,
        query_port      => QueryPort,
        vector          => Vec,
        max_peers       => 100,
        gossip_interval => 5_000
    }),
    lists:foreach(
        fun({H, P}) -> catch em_pop_node:add_peer(PopPid, H, P) end,
        Seeds),
    Dispatch = cowboy_router:compile([
        {'_', [{"/agent/query", em_filter_http,
                #{server => pubmed_filter_server}}]}
    ]),
    {ok, _} = cowboy:start_clear(pubmed_filter_query_listener,
                                  [{port, QueryPort}],
                                  #{env => #{dispatch => Dispatch}}),
    logger:notice("[pubmed_filter] gossip port ~w  query port ~w",
                  [PopPort, QueryPort]),
    ok.

handle(Body, Memory) when is_binary(Body) ->
    {generate_embryo_list(Body), Memory};
handle(_Body, Memory) ->
    {[], Memory}.

%%====================================================================
%% Search pipeline: esearch → esummary
%%====================================================================

generate_embryo_list(JsonBinary) ->
    {Query, Timeout} = extract_params(JsonBinary),
    case fetch_ids(Query, Timeout) of
        []  -> [];
        Ids -> fetch_summaries(Ids, Timeout)
    end.

extract_params(JsonBinary) ->
    try json:decode(JsonBinary) of
        Map when is_map(Map) ->
            Query   = binary_to_list(maps:get(<<"value">>, Map,
                          maps:get(<<"query">>, Map, <<"">>))),
            Timeout = case maps:get(<<"timeout">>, Map, undefined) of
                undefined            -> 15;
                T when is_integer(T) -> T;
                T when is_binary(T)  -> binary_to_integer(T)
            end,
            {Query, Timeout};
        _ ->
            {binary_to_list(JsonBinary), 15}
    catch
        _:_ -> {binary_to_list(JsonBinary), 15}
    end.

fetch_ids("", _) -> [];
fetch_ids(Query, Timeout) ->
    Url = lists:flatten(io_lib:format("~s~s", [?ESEARCH_URL, uri_string:quote(Query)])),
    Headers = [{"User-Agent", "pubmed_filter/1.0"}],
    case httpc:request(get, {Url, Headers},
                       [{timeout, Timeout * 1000},
                        {ssl, [{verify, verify_none}]}],
                       [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            parse_ids(Body);
        _ ->
            []
    end.

parse_ids(JsonBin) ->
    try json:decode(JsonBin) of
        #{<<"esearchresult">> := #{<<"idlist">> := Ids}} when is_list(Ids) ->
            [binary_to_list(Id) || Id <- Ids, is_binary(Id)];
        _ ->
            []
    catch
        _:_ -> []
    end.

fetch_summaries([], _) -> [];
fetch_summaries(Ids, Timeout) ->
    IdStr = string:join(Ids, ","),
    Url   = lists:flatten(io_lib:format("~s~s", [?ESUMMARY_URL, IdStr])),
    Headers = [{"User-Agent", "pubmed_filter/1.0"}],
    case httpc:request(get, {Url, Headers},
                       [{timeout, Timeout * 1000},
                        {ssl, [{verify, verify_none}]}],
                       [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            parse_summaries(Ids, Body);
        _ ->
            []
    end.

parse_summaries(Ids, JsonBin) ->
    try json:decode(JsonBin) of
        #{<<"result">> := Result} when is_map(Result) ->
            lists:filtermap(fun(Id) ->
                IdBin = list_to_binary(Id),
                case maps:get(IdBin, Result, undefined) of
                    undefined -> false;
                    Article   -> build_embryo(Id, Article)
                end
            end, Ids);
        _ ->
            []
    catch
        _:_ -> []
    end.

build_embryo(Id, #{<<"title">> := Title} = Article) ->
    Url     = lists:flatten(io_lib:format("https://pubmed.ncbi.nlm.nih.gov/~s/", [Id])),
    Authors = format_authors(maps:get(<<"authors">>, Article, [])),
    Source  = maps:get(<<"source">>,  Article, <<"">>),
    PubDate = maps:get(<<"pubdate">>, Article, <<"">>),
    Resume  = format_resume(Authors, Source, PubDate),
    {true, #{
        <<"properties">> => #{
            <<"url">>    => list_to_binary(Url),
            <<"resume">> => list_to_binary(Resume),
            <<"title">>  => Title,
            <<"pmid">>   => list_to_binary(Id),
            <<"source">> => <<"pubmed.ncbi.nlm.nih.gov">>
        }
    }};
build_embryo(_, _) ->
    false.

format_authors([]) -> "";
format_authors(Authors) when is_list(Authors) ->
    Names = lists:filtermap(fun(A) ->
        case maps:get(<<"name">>, A, undefined) of
            undefined -> false;
            N         -> {true, binary_to_list(N)}
        end
    end, Authors),
    string:join(lists:sublist(Names, 3), ", ").

format_resume(Authors, Source, PubDate) ->
    Parts = [P || P <- [Authors,
                         binary_to_list(Source),
                         binary_to_list(PubDate)],
                  P =/= ""],
    string:join(Parts, " — ").
