#!/bin/bash


message="${@:1}"

echo "send message $message"

curl -X POST --data-urlencode "payload={\"channel\": \"#disagg-eval\", \"username\": \"ubuntu\", \"text\": \"$message.\", \"icon_emoji\": \":fries:\"}" https://hooks.slack.com/services/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
