
%% ------------------------------------------------------------------
%% twitterminer_crunchtags is an OTP gen_server that handles all 
%% the data preprocessing of the already aggregated tag data. 
%% It is built as a gen_server to take advantage of the 
%% supervision and message handling capabilities as it must not go down.
%% It ensures that future quries for tag distribution data are fast and efficient
%% ------------------------------------------------------------------

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, RiakIP, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% Establishes connection to riak node on local machine. 
%% Crashes if no connection can be made and OTP failover kicks in when node fails. 

init([RiakIP]) ->
	io:format("Starting crunch tags"),
	try riakc_pb_socket:start_link(RiakIP, 8087) of
    {ok, RiakPID} ->
    	{ok, {RiakPID,[]}}
	
	catch
    	_ ->
      		exit("no_riak_connection")
  	end.

%% receives a trigger from the tick timer function once a minute

handle_cast(tick, {RiakPID, OldTags}) ->
  %get current list of available tags
  ListOfTags = case riakc_pb_socket:get(RiakPID, <<"taglistbucket">>, <<"taglist">>) of
    {ok, List} -> 
        FinalTaglist = binary_to_term(riakc_obj:get_value(List)),  
        FinalTaglist;
    Reason -> exit(Reason)
  end,

  % get list of keys up to 40 mins old
  AllKeys = case riakc_pb_socket:get_index_range(
            RiakPID,
            <<"tags">>, %% bucket name
            {integer_index, "timestamp"}, %% index name
            oldTimeStamp(), timeStamp()
          ) of
    {ok, {_,TempKeys,_,_}} ->
      Keys = lists:reverse(lists:sort(TempKeys)),
      Keys;
    Reason1 -> exit(Reason1)
  end,
  
  %map over the keys for values 
  TagDicts = get_dicts(AllKeys, RiakPID),
  %map over the tags using the dicts to constuct the processed data and put to riak
  CrunchedTags = lists:map(fun(Tag) -> crunch_tags(Tag, TagDicts) end, ListOfTags),
  put_to_riak(CrunchedTags, RiakPID),
  %delete old tags to keep directory clean 
  %(it builds up veeeery quickly as there are a lot of one use tags)
  DeleteTags = gb_sets:to_list(gb_sets:difference(gb_sets:from_list(OldTags), gb_sets:from_list(ListOfTags))),
  lists:foreach(fun(DeleteTag) -> delete(DeleteTag, RiakPID) end, DeleteTags),
  {noreply, {RiakPID, ListOfTags}};
	
%% Required callbacks

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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


get_obj(Key, RiakPID) ->
  {ok, Obj} = riakc_pb_socket:get(RiakPID, <<"tags">>, Key), 
  Obj.

get_value(Object) ->
  Value = binary_to_term(riakc_obj:get_value(Object)), 
  Value.

%% extract the dict structues from the Riak objects
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

%% -----------------------------------
%% convert data from list of dicts to single tag distribution, cotag and tweet JSON structure
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
%% -----------------------------------


%% push to riak database
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


  