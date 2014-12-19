%% ------------------------------------------------------------------
%% twitterminer_tickloop handles the timing of the preprocessing jobs, 
%% these are processor heavy and so are kept to a non overlapping shedule
%% ------------------------------------------------------------------

-module(twitterminer_tickloop).


-export([start/0]).

%% ===================================================================
%% Module Functions
%% ===================================================================

start() ->
	io:format("Starting tickloop"),
	Pid = spawn_link(fun() -> tickLoop() end),
	register(twitterminer_tickloop, Pid),
	io:format("continuing tickloop"),
	{ok, Pid}.

tickLoop() ->
	timer:sleep(30000),
	gen_server:cast(twitterminer_riak, tick),
	timer:sleep(15000),
	gen_server:cast(twitterminer_crunchtags, tick),
	timer:sleep(15000),
	gen_server:cast(twitterminer_updatelist, tick),
	tickLoop().
