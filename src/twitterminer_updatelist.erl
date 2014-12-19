%% ------------------------------------------------------------------
%% twitterminer_updatelist is an OTP gen_server that handles the indexing of the 
%% "available" tags from the aggregated tag data. 
%% It is built as a gen_server to take advantage of the 
%% supervision and message handling capabilities as it must not go down.
%% It requires a map/fold (or map reduce) pattern to obtain all available tags, 
%% making sure they are unique and reducing them down to a single list
%% we have not distributed this for the time being as the overall 
%% architecture and constraints of our hardware are better suited to sequential processing
%% ------------------------------------------------------------------

-module(twitterminer_updatelist).
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

%% Establishes connection to riak node on local machine. 
%% Crashes if no connection can be made and OTP failover kicks in when node fails.

init([RiakIP]) ->
	io:format("Starting riak conn update list"),
	try riakc_pb_socket:start_link(RiakIP, 8087) of
    {ok, RiakPID} ->
    	{ok, RiakPID}
	catch
    	_ ->
      		exit("no_riak_connection")
  	end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% Functionality: When the atom tick is recieved from the tickloop, calls 20 minutes of 
%% data from riak and runs a map then a fold function to reduce this down to a 
%% single list of available tags

handle_cast(tick, RiakPID) ->
	case riakc_pb_socket:get_index_range(
          RiakPID,
          <<"tags">>, %% bucket name
          {integer_index, "timestamp"}, %% index name
          oldTimeStamp(), timeStamp() 
        ) of
		{ok, {_,Keys,_,_}} ->
			if 
				length(Keys) < 20 ->
					NewKeys = lists:reverse(lists:sort(Keys)),
					%% Map
					Objects = lists:map(fun(Key) -> {ok, Obj} = riakc_pb_socket:get(RiakPID, <<"tags">>, Key), Obj end, NewKeys),
					%%Reduce
					Tagset = lists:foldl(fun(Object, Alltags) -> Value = binary_to_term(riakc_obj:get_value(Object)), 
						Tags = dict:fetch_keys(Value), gb_sets:union([Alltags, gb_sets:from_list(Tags)]) end, gb_sets:new(), Objects),
					TaglistVal = term_to_binary(gb_sets:to_list(Tagset)),
					case riakc_pb_socket:get(RiakPID, <<"taglistbucket">>, <<"taglist">>) of
						{ok, OldTaglist} ->			
							NewTaglist = riakc_obj:update_value(OldTaglist, TaglistVal);
						{error,_} ->
							NewTaglist = riakc_obj:new(<<"taglistbucket">>,
									        <<"taglist">>,
									        TaglistVal)
					end,
					riakc_pb_socket:put(RiakPID, NewTaglist);
				true ->
					{NewKeys,_} = lists:split(20, lists:reverse(lists:sort(Keys))),
					%% Map
					Objects = lists:map(fun(Key) -> {ok, Obj} = riakc_pb_socket:get(RiakPID, <<"tags">>, Key), Obj end, NewKeys),
					%%Reduce
					Tagset = lists:foldl(fun(Object, Alltags) -> Value = binary_to_term(riakc_obj:get_value(Object)), 
						Tags = dict:fetch_keys(Value), gb_sets:union([Alltags, gb_sets:from_list(Tags)]) end, gb_sets:new(), Objects),
					TaglistVal = term_to_binary(gb_sets:to_list(Tagset)),
					case riakc_pb_socket:get(RiakPID, <<"taglistbucket">>, <<"taglist">>) of
						{ok, OldTaglist} ->			
							NewTaglist = riakc_obj:update_value(OldTaglist, TaglistVal);
						{error,_} ->
							NewTaglist = riakc_obj:new(<<"taglistbucket">>,
									        <<"taglist">>,
									        TaglistVal)
					end,
					riakc_pb_socket:put(RiakPID, NewTaglist)
			end,
			{noreply, RiakPID};
		{timeout, Reason} ->
			io:format("Timeout of secondary index search"),
			exit({timeout, Reason});
		{error,notfound} ->
			{noreply, RiakPID};
		{error, Reason} ->
		    exit(Reason)
	end;


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

timeStamp() ->
	{Mega, Secs, _} = erlang:now(),
	Mega*1000*1000 + Secs.

oldTimeStamp() ->
	{Mega, Secs, _} = erlang:now(),
	Mega*1000*1000 + (Secs - 2400).