-module(twitterminer_riaksup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
%% -define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(RiakIP) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [RiakIP]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(RiakIP) ->
    {ok, { {one_for_one, 10, 5000}, 
    	[{twitterminer_riak,
             {twitterminer_riak, start_link, [RiakIP]},
             permanent,
             5000, 
             worker,
             [twitterminer_riak]},
        {twitterminer_updatelist,
             {twitterminer_updatelist, start_link, [RiakIP]},
             permanent,
             5000, 
             worker,
             [twitterminer_updatelist]},
        {twitterminer_tickloop,
             {twitterminer_tickloop, start, []},
             permanent,
             5000, 
             worker,
             [twitterminer_tickloop]}
            ]} }.
