# Twitterminer

## This is Team QI's Riak twitterminer and data cruncher.

### Installation

1.  Get Erlang

    You really need an Erlang installation to run this project.

1.  Get Rebar

    Rebar is a build script for Erlang projects. You may install it from your distribution packages, or get it from here:

    https://github.com/basho/rebar

1.  Clone the repo

        $ git clone https://github.com/SEM-Qi/twitterminer.git

1.  Get the dependencies

        $ cd dit029-twitter-miner/
        $ rebar get-deps

1.  Compile the dependencies and the package

        $ rebar compile

1.  If not already done, get a Twitter account and generate authentication keys
    (We have this already, check the Google drive under technical documentation. Be aware that I dont know if we can simultaneously access the stream with these credentials)

    1.  Open a Twitter account at https://twitter.com .

    1.  Go to https://apps.twitter.com and create a new app.

    1.  Generate API keys for the app using the API Keys tab, as described
        [here](https://dev.twitter.com/oauth/overview/application-owner-access-tokens).

    1.  Collect the `API key`, `API secret`, `Access token` and `Access token secret`,
        and put them into the `twitterminer.config` file, which you find in the repo's
        toplevel directory.

1.  Run the example

    Run the Erlang shell from the repo's toplevel directory with additional library path and configuration flags- NB: The config will be different depending on the server you deploy on! options are: `config/picard`, `config/greedo`, `config/bobafett`, `config/garak`, `config/amazon`

        $ erl -pa deps/*/ebin -pa ebin -config config/picard -name twitterminer -setcookie tagwars

    Start all needed Erlang applications in the shell

    ```erlang
    1> application:ensure_all_started(twitterminer).
    ```

    Note that the previous step requires Erlang/OTP 16B02 or newer. 

    If everything goes OK, you should see `{ok,[ibrowse,crypto,asn1,public_key,ssl,twitterminer]}` a number of processes reporting startup and finally a response header from Twitter. If you get a message indicating HTTP response code 401, it probably means authentication error. Due to a known bug (unrelated to any HTTP error), please restart the shell before attempting to reconnect to Twitter.

### Functionality

Recieves a stream of tweets from Twitters public stream API, parses that data, to pull tag and tweet information, tracks frequency of tag use by minute and, once a minute, puts the distribution data to riak. It also processes the list of available tags and the current tag distribution data based on the available realtime data (fudged to a 120:1 ratio to account for sparse data and offset by 20 minutes for best response times when accounting for same 120:1 ratio) 

### Dependencies

#### [erlang-oauth](https://github.com/tim/erlang-oauth/)

erlang-oauth is used to construct signed request parameters required by OAuth.

#### [ibrowse](https://github.com/cmullaparthi/ibrowse)

ibrowse is an HTTP client allowing to close a connection while the request is still being serviced. We need this for cancelling Twitter streaming API requests.

#### [jiffy](https://github.com/davisp/jiffy)

jiffy is a JSON parser, which uses efficient C code to perform the actual parsing. [mochijson2](https://github.com/bjnortier/mochijson2) is another alternative that could be used here.

### Authors

* Team QI based heavily off [DIT029 Twitter Miner](https://github.com/michalpalka/dit029-twitter-miner) by Michał Pałka (michalpalka) <michal.palka@chalmers.se>

