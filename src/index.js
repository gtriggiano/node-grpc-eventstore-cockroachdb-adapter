import pg from 'pg'
import fs from 'fs'
import path from 'path'

import AppendEvents from './api/AppendEvents'
import GetEvents from './api/GetEvents'
import GetEventsByStream from './api/GetEventsByStream'
import GetEventsByStreamType from './api/GetEventsByStreamType'

import validateConfiguration from './helpers/validateConfiguration'

let defaultConfig = {
  host: 'localhost',
  port: 26257,
  database: 'eventstore',
  table: 'events',
  user: 'root',
  maxPoolClients: 10,
  idleTimeoutMillis: 10000,
  batchSize: 500,
  ssl: undefined,
}

export default function CockroachDBAdapter (config = {}) {
  config = {...defaultConfig, ...config}
  validateConfiguration(config)

  let pool = new pg.Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    max: config.maxPoolClients,
    idleTimeoutMillis: config.idleTimeoutMillis,
    ...(config.ssl ? {ssl: config.ssl} : {}),
  })

  let db = {}
  return Object.defineProperties(db, {
    pool: {
      value: pool,
    },
    table: {
      value: config.table,
    },
    batchSize: {
      value: config.batchSize,
    },
    appendEvents: {
      value: AppendEvents(db),
    },
    getEvents: {
      value: GetEvents(db),
    },
    getEventsByStream: {
      value: GetEventsByStream(db),
    },
    getEventsByStreamType: {
      value: GetEventsByStreamType(db),
    },
  })
}

export function createTableSQL (table = 'events') {
  let sql = fs.readFileSync(path.resolve(__dirname, 'createTable.sql'), 'utf8')
  return sql.replace('events', table)
}
