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

init([RiakIP]) ->
	io:format("Starting riak conn update list"),
	try riakc_pb_socket:start_link(RiakIP, 8087) of
    {ok, RiakPID} ->
    	{ok, {RiakPID, RiakIP}}
	catch
    	_ ->
      		exit("no_riak_connection")
  	end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.
%%potential for more efficiency if only the first and last are operated on, maybe using dict:fold. Maybe...
handle_cast(tick, {RiakPID, RiakIP}) ->
	{ok, Keys} = riakc_pb_socket:list_keys(RiakPID, <<"tags">>),
	case riakc_pb_socket:list_keys(RiakPID, <<"tags">>) of
		{ok, Keys} ->
			if 
				length(Keys) < 20 ->
					ok;
				true ->
					{NewKeys,_} = lists:split(20, lists:reverse(lists:sort(Keys))),
					Objects = lists:map(fun(Key) -> {ok, Obj} = riakc_pb_socket:get(RiakPID, <<"tags">>, Key), Obj end, NewKeys),
					Tagset = lists:foldl(fun(Object, Alltags) -> Value = binary_to_term(riakc_obj:get_value(Object)), Tags = dict:fetch_keys(Value), sets:union([Alltags, sets:from_list(Tags)]) end, sets:new(), Objects),
					TaglistVal = term_to_binary(sets:to_list(Tagset)),
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
			{noreply, {RiakPID, RiakIP}};
		{error,<<"timeout">>} ->
			io:format("list keys timed out"),
			riakc_pb_socket:stop(RiakPID),
			try riakc_pb_socket:start_link(RiakIP, 8087) of
			    {ok, NewRiakPID} ->
			    	{noreply, {NewRiakPID, RiakIP}};
			catch
		    	_ ->
		      		exit("no_riak_connection")
		  	end

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

