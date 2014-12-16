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
    gen_server:start_link({global, ?SERVER}, ?MODULE, RiakIP, []).

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
			NewStore = dict:store(Tag, {1, gb_sets:from_list(Cotags),gb_sets:add_element(Tweet, gb_sets:new())}, Store);
		{ok, {N, OldCotags, OldTweets}} ->
			NewStore = dict:store(Tag, {N+1, gb_sets:union(OldCotags, gb_sets:from_list(Cotags)),gb_sets:add_element(Tweet, OldTweets)}, Store)
	end,
    {noreply, {RiakPID, NewStore}};

handle_cast(tick, {RiakPID, Store}) ->
	spawn_link(fun() -> createRiakObj(Store, RiakPID) end),
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
	MD1 = riakc_obj:get_update_metadata(Obj),
	MD2 = riakc_obj:set_secondary_index(MD1, [{{integer_index, "timestamp"}, [timeStamp()]}]),
	TagObj = riakc_obj:update_metadata(Obj, MD2),
	riakc_pb_socket:put(RiakPID, TagObj),
	case riakc_pb_socket:get_index_range(
	          RiakPID,
	          <<"tags">>, %% bucket name
	          {integer_index, "timestamp"}, %% index name
	          oldestTimeStamp(), oldTimeStamp() 
	        ) of
		{ok, {_,Keys,_,_}} ->
			Deletefrom = oldTimeStamp(),
			OldKeys = lists:filter(fun(Key) -> case binary_to_integer(Key) of
													IntKey when IntKey > Deletefrom ->
														false;
													_ ->
														true
												end end, Keys),
			lists:map(fun(OldKey) -> riakc_pb_socket:delete(RiakPID, <<"tags">>, OldKey) end, OldKeys),
			ok;
		{timeout, Reason} -> exit({timeout, Reason});
		{error, Reason} -> exit(Reason)
	end.


timeStamp() ->
	{Mega, Secs, _} = erlang:now(),
	Mega*1000*1000 + Secs.

oldTimeStamp() ->
	{Mega, Secs, _} = erlang:now(),
	Mega*1000*1000 + (Secs - 2400).

oldestTimeStamp() ->
	{Mega, _, _} = erlang:now(),
	Mega*1000*1000.

