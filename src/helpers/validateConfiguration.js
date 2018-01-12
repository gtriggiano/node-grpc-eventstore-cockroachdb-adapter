import assert from 'assert'
import {
  isString,
  isInteger,
  isEmpty,
  isNil,
  isObject,
} from 'lodash'

let msg = (str) => `[CockroachDBAdapter config validation]: ${str}`

export default function validateConfiguration (config) {
  let {
    host,
    port,
    database,
    table,
    user,
    maxPoolClients,
    idleTimeoutMillis,
    batchSize,
    ssl,
  } = config

  assert(
    isString(host) && !isEmpty(host.trim()),
    msg(`config.host is a non empty string. Received ${JSON.stringify(host)}`)
  )
  assert(
    isInteger(port) &&
    port > 1 &&
    port <= 65535,
    msg(`config.port is a valid port number. Received ${JSON.stringify(port)}`)
  )
  assert(
    isString(database) && !isEmpty(database.trim()),
    msg(`config.database is a non empty string. Received ${JSON.stringify(database)}`)
  )
  assert(
    isString(table) && !isEmpty(table.trim()),
    msg(`config.table is a non empty string. Received ${JSON.stringify(table)}`)
  )
  assert(
    isString(user) && !isEmpty(user.trim()),
    msg(`config.user is a non empty string. Received ${JSON.stringify(user)}`)
  )
  assert(
    isInteger(maxPoolClients) && maxPoolClients > 1,
    msg(`config.maxPoolClients is a positive integer. Received ${JSON.stringify(maxPoolClients)}`)
  )
  assert(
    isInteger(idleTimeoutMillis) && idleTimeoutMillis >= 1000,
    msg(`config.idleTimeoutMillis is a positive integer higher than 999. Received ${JSON.stringify(idleTimeoutMillis)}`)
  )
  assert(
    isInteger(batchSize) && batchSize > 49,
    msg(`config.batchSize is a positive integer higher than 49. Received ${JSON.stringify(batchSize)}`)
  )
  assert(
    isNil(ssl) ||
    (
      isObject(ssl) &&
      isString(ssl.ca) && !isEmpty(ssl.ca.trim()) &&
      isString(ssl.cert) && !isEmpty(ssl.cert.trim()) &&
      isString(ssl.key) && !isEmpty(ssl.key.trim())
    ),
    msg(`config.ssl is either nil or an object with the following shape: {ca: String, cert: String, key: String}. Received ${JSON.stringify(ssl)}`)
  )
}
