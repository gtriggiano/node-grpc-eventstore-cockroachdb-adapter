FROM node:8.9.0-slim

WORKDIR /package

# Install dependencies
COPY package.json package.json
COPY yarn.lock yarn.lock
ADD .yarn-cache.tgz /
RUN yarn

# Add node_modules/.bin to PATH
ENV PATH "/package/node_modules/.bin:${PATH}"
