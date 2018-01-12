#! /bin/bash

# Init empty cache file
if [ ! -f .yarn-cache.tgz ]; then
  echo "Init empty .yarn-cache.tgz"
  tar cvzf .yarn-cache.tgz --files-from /dev/null
fi

if [ ! -f yarn.lock ]; then
  echo "Init empty yarn.lock"
  touch yarn.lock
fi

docker-compose build

echo "Saving yarn.lock"
docker run --rm --entrypoint cat grpc-eventstore-cockroachdb-adapter:development /package/yarn.lock > yarn.lock
echo "Saving Yarn cache"
docker run --rm --entrypoint tar grpc-eventstore-cockroachdb-adapter:development czf - /usr/local/share/.cache/yarn/v1 > .yarn-cache.tgz
