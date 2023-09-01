#!/usr/bin/env bash

DIR=`dirname "$0"`

rm -rf ~/.wasmd

# initial new node
wasmd init validator --chain-id alles-1
echo "lock nasty suffer dirt dream fine fall deal curtain plate husband sound tower mom crew crawl guard rack snake before fragile course bacon range" \
    | wasmd keys add validator --recover --keyring-backend test
echo "smile stem oven genius cave resource better lunar nasty moon company ridge brass rather supply used horn three panic put venue analyst leader comic" \
    | wasmd keys add requester --recover --keyring-backend test

cp ./docker-config/single-validator/priv_validator_key.json ~/.wasmd/config/priv_validator_key.json
cp ./docker-config/single-validator/node_key.json ~/.wasmd/config/node_key.json

# add accounts to genesis
wasmd genesis add-genesis-account validator 10000000000000stake --keyring-backend test
wasmd genesis add-genesis-account requester 10000000000000stake --keyring-backend test


# register initial validators
wasmd genesis gentx validator 100000000stake \
    --chain-id alles-1 \
    --keyring-backend test

# collect genesis transactions
wasmd genesis collect-gentxs

# change deposit and voting periods to 1 minuite
sed -i 's/172800s/60s/' ~/.wasmd/config/genesis.json

rm -r docker-config/config
mkdir docker-config/config
cp ~/.wasmd/config/app.toml docker-config/config
cp ~/.wasmd/config/config.toml docker-config/config
cp ~/.wasmd/config/genesis.json docker-config/config
