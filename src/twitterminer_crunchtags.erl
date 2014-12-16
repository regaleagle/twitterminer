-module(twitterminer_crunchtags).
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
	io:format("Starting crunch tags"),
	try riakc_pb_socket:start_link(RiakIP, 8087) of
    {ok, RiakPID} ->
    io:format("~s~n", [RiakPID]),
    	{ok, {RiakPID,[]}}
	
	catch
    	_ ->
      		exit("no_riak_connection")
  	end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% Pseudo code for twitterminer_crunchtags. Do map and reduce on a continuous timed loop on data that twitterminer_updatelist has gathered. 	
%% -receive signal (or "tick") in a handle_cast function (look at the existing modules)
handle_cast(tick, {RiakPID, OldTags}) ->
  ListOfTags = case riakc_pb_socket:get(RiakPID, <<"taglistbucket">>, <<"taglist">>) of
    {ok, List} -> 
        FinalTaglist = binary_to_term(riakc_obj:get_value(List)),  
        FinalTaglist;
    Reason -> exit(Reason)
  end,
  AllKeys = case riakc_pb_socket:get_index_range(
            RiakPID,
            <<"tags">>, %% bucket name
            {integer_index, "timestamp"}, %% index name
            oldTimeStamp(), timeStamp() %% origin timestamp should eventually have some logic attached
          ) of
    {ok, {_,TempKeys,_,_}} ->
      Keys = lists:reverse(lists:sort(TempKeys)),
      Keys;
    Reason1 -> exit(Reason1)
  end,
  TagDicts = get_dicts(AllKeys, RiakPID),
  CrunchedTags = lists:map(fun(Tag) -> crunch_tags(Tag, TagDicts) end, ListOfTags),
  put_to_riak(CrunchedTags, RiakPID),
  DeleteTags = gb_sets:to_list(gb_sets:difference(gb_sets:from_list(OldTags), gb_sets:from_list(ListOfTags))),
  lists:foreach(fun(DeleteTag) -> delete(DeleteTag, RiakPID) end, DeleteTags),
  {noreply, {RiakPID, ListOfTags}};
  

%% Instead of returning a callback for the data server at the end of the function, you put that data structure in riak, as above.


%% -wait for next tick. This will happen automatically as you should be using an OTP gen_server. 
	



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

get_dicts(AllKeys, RiakPID) when length(AllKeys) >= 20 ->
  {NewKeys,_} = lists:split(20, AllKeys),
  Objects = lists:map(fun(Key) ->  get_obj(Key, RiakPID) end, NewKeys),
  lists:map(fun get_value/1, Objects);
  
get_dicts(AllKeys, RiakPID) when (length(AllKeys) >= 2) and (length(AllKeys) rem 2 =:= 0) ->
  Objects = lists:map(fun(Key) ->  get_obj(Key, RiakPID) end, AllKeys),
  lists:map(fun get_value/1, Objects);

get_dicts(AllKeys, RiakPID) when length(AllKeys) >= 2 ->
  [_|NewKeys] = AllKeys,
  Objects = lists:map(fun(Key) ->  get_obj(Key, RiakPID) end, NewKeys),
  lists:map(fun get_value/1, Objects);

get_dicts(_, _) ->
  empty.

get_obj(Key, RiakPID) ->
  {ok, Obj} = riakc_pb_socket:get(RiakPID, <<"tags">>, Key), 
  Obj.

get_value(Object) ->
  Value = binary_to_term(riakc_obj:get_value(Object)), 
  Value.


crunch_tags(Tag, empty) -> 
  {Distribution, Cotags}  = {[{[{<<"numtags">>, 0}, {<<"tweets">>, ""}]}],[]},
  Value = jiffy:encode({[{<<"tag">>, Tag},
  {<<"cotags">>, Cotags},
  {<<"distribution">>, 
    Distribution}]}),
  {Tag, Value};

crunch_tags(Tag, TagDicts) ->
  Tagset = lists:map(fun(Dict) -> get_dist(Tag, Dict) end, TagDicts),
  {Distribution, Cotags} = loopThrough(Tagset, [], gb_sets:new()),
  Value = jiffy:encode({[{<<"tag">>, Tag},
  {<<"cotags">>, Cotags},
  {<<"distribution">>, 
    Distribution}]}),
  {Tag, Value}.

get_dist(Tag, TagDict) ->
  case dict:find(Tag, TagDict) of 
    {ok, Tagged} -> Tagged; 
    error -> {0, gb_sets:new(),gb_sets:new()} 
  end.


loopThrough([], L, Cotags) -> {L, gb_sets:to_list(Cotags)};
loopThrough(Tagset, L, OldCotags) ->
  {NewKeys,OldKeys} = lists:split(2, Tagset),
  [{Num, Cotags, Tweets}, {Num2, Cotags2, Tweets2}] = NewKeys,
  L2 = [{[{<<"numtags">>, Num + Num2}, {<<"tweets">>, gb_sets:to_list(gb_sets:union([Tweets, Tweets2]))}]}|L],
  NewCotags = gb_sets:union([Cotags, Cotags2, OldCotags]),
  loopThrough(OldKeys, L2, NewCotags).

put_to_riak([], _) -> ok; 
put_to_riak([H|T], RiakPid) -> 
  {Tag, Value} = H,
  PutTagValue = riakc_obj:new(<<"formattedtweets">>,
                        Tag,
                        term_to_binary(Value)),

  riakc_pb_socket:put(RiakPid, PutTagValue),
  put_to_riak(T, RiakPid).

delete(DeleteTag, RiakPID) ->
  riakc_pb_socket:delete(RiakPID, <<"formattedtweets">>, DeleteTag).
  

timeStamp() ->
  {Mega, Secs, _} = erlang:now(),
  Mega*1000*1000 + Secs.

oldTimeStamp() ->
  {Mega, Secs, _} = erlang:now(),
  Mega*1000*1000 + (Secs - 2400).


  