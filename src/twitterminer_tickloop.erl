-module(twitterminer_tickloop).


-export([start/0]).

%% ===================================================================
%% Module Functions
%% ===================================================================

start() ->
	io:format("Starting tickloop"),
	Pid = spawn_link(fun() -> tickLoop() end),
	global:register_name(twitterminer_tickloop, Pid),
	io:format("continuing tickloop"),
	{ok, Pid}.

tickLoop() ->
	timer:sleep(30000),
	io:format("now ticking"),
	gen_server:cast(twitterminer_riak, tick),
	timer:sleep(15000),
	gen_server:cast(twitterminer_crunchtags, tick),
	timer:sleep(15000),
	gen_server:cast(twitterminer_updatelist, tick),
	tickLoop().
