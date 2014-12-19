%% ------------------------------------------------------------------
%% twitterminer_app is an OTP application that handles the bulk of the data 
%% processing in the background so the client facing components of the data 
%% backend don't have to
%% ------------------------------------------------------------------

-module(twitterminer_app).

-behaviour(application).

%% Application callbacks

-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================


%% startup procedures for both normal (also used for failover) and takeover startups

start(normal, []) ->
    twitterminer_sup:start_link();
start({takeover, _OtherNode}, []) ->
    twitterminer_sup:start_link().

stop(_State) ->
    ok.
