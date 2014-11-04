-module(twitterminer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, 5, 1000}, 
    	[{twitterminer_tweets,
            {twitterminer_tweets, start_link, []},
            permanent,
            5000, 
            worker,
            [twitterminer_tweets]},
        {twitterminer_riaksup,
            {twitterminer_riaksup, start_link, ["picard.skip.chalmers.se"]},  % A = Get list of connections from refServer
            permanent,
            5000, 
            supervisor,
            [twitterminer_riaksup]},
        {twitterminer_source,
            {twitterminer_source, start_link, []},
            permanent,
            5000, 
            worker,
            [twitterminer_source]}
        ]} }.

