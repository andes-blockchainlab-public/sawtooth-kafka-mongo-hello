#!/bin/bash

gnome-terminal --tab -- bash -c "cd ./docker-compose; ./down.sh; ./up.sh; bash"

sleep 10
gnome-terminal --tab -- bash -c "cd ./tp1; npm start; bash"
gnome-terminal --tab -- bash -c "cd ./app; node ./sawtooth-sub-events.js; bash"

sleep 1
gnome-terminal --tab -- bash -c "cd ./app; 
  echo \"--sawtooth--\";
  node ./sawtooth-post.js; 
  sleep 1; 
  node ./sawtooth-get.js; 
  echo \"--mongo--\";
  node ./mongo-sample.js;
  echo \"--kafka--\";
  node ./kafka.js;
  bash"


firefox http://localhost:8081 http://localhost:8091 http://localhost:8008/blocks &