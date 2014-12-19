%% ------------------------------------------------------------------
%% twitterminer_sup is an OTP supervisor that sits at the top of the supervisor tree. 
%% It starts a number of permanent processes in its init stage including 
%% another supervisor and supervises them. 
%% These processes are registered and should never die (i.e. will be restarted). 
%% ------------------------------------------------------------------

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
    {ok, Server} = application:get_env(server),
    {ok, { {one_for_one, 5, 1000}, 
    	[{twitterminer_riaksup,
            {twitterminer_riaksup, start_link, [Server]}, 
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

