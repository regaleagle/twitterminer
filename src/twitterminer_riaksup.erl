-module(twitterminer_riaksup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

start_link(RiakIP) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [RiakIP]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================




init(RiakIP) ->
%% Here is where you add any new process to be started at runtime as per the below. 
%% You can use twitterminer_updatelist as a template

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
