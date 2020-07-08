#!/bin/bash

now=$(date '+%Y%m%d%H%M%S')
token=${token:-""}
curl -H "Authorization: Bearer ${token}" -H "Content-Type: application/x-www-form-urlencoded" https://api-fxpractice.oanda.com/v3/instruments/USD_JPY/candles?price=B > $now.json
