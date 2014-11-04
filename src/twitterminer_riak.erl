-module(twitterminer_riak).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(RiakIP) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, RiakIP, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([RiakIP]) ->
	io:format("Starting riak conn ~s~n", [RiakIP]),
	try riakc_pb_socket:start_link(RiakIP, 8087) of
    	{ok, RiakPID} ->
    		{ok, {RiakPID, dict:new()}}
	catch
    	_ ->
      		exit("no_riak_connection")
  	end.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({store, Tag, Cotags, Tweet}, {RiakPID, Store}) ->
	case dict:find(Tag, Store) of
		error ->
			NewStore = dict:store(Tag, {1, sets:from_list(Cotags),sets:add_element(Tweet, sets:new())}, Store);
		{ok, {N, OldCotags, OldTweets}} ->
			NewStore = dict:store(Tag, {N+1, sets:union(OldCotags, sets:from_list(Cotags)),sets:add_element(Tweet, OldTweets)}, Store)
	end,
    {noreply, {RiakPID, NewStore}};

handle_cast(tick, {RiakPID, Store}) ->
	spawn(fun() -> createRiakObj(Store, RiakPID) end),
    {noreply, {RiakPID, dict:new()}};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------


createRiakObj(Value, RiakPID) ->
	Obj = riakc_obj:new(<<"tags">>,
        integer_to_binary(timeStamp()),
        term_to_binary(Value)),
	riakc_pb_socket:put(RiakPID, Obj),
	{ok, Keys} = riakc_pb_socket:list_keys(RiakPID, <<"tags">>),
	Deletefrom = oldTimeStamp(),
	OldKeys = lists:filter(fun(Key) -> case binary_to_integer(Key) of
											IntKey when IntKey > Deletefrom ->
												false;
											_ ->
												true
										end end, Keys),
	lists:map(fun(OldKey) -> riakc_pb_socket:delete(RiakPID, <<"tags">>, OldKey), OldKeys),
	ok.


timeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

oldTimeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + ((Secs - 2400) * 1000 * 1000) + Micro.

