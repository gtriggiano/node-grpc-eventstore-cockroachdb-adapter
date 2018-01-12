import path from 'path'

import pool from './connectionPool'

let codeFolder = path.resolve(__dirname, '..', '..', process.env.CODE_FOLDER)
let createTableSQL = require(codeFolder).createTableSQL
let tableCreationSQL = createTableSQL()

export default function createEventsTable () {
  return pool.connect()
    .then((client) =>
      client
      .query(tableCreationSQL)
      .then(() => client.release())
      .catch((e) => {
        client.release()
        throw e
      })
    )
}
