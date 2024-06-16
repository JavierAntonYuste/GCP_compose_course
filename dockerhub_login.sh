#!/bin/bash
REGISTRY_URL="https://index.docker.io/v1/"
USERNAME="javianton97"
read -s -p "Enter Docker Hub personal access token: " ACCESS_TOKEN
echo "$ACCESS_TOKEN" | docker login $REGISTRY_URL -u $USERNAME --password-stdin
unset ACCESS_TOKEN
