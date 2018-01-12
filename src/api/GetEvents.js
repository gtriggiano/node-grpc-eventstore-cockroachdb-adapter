import EventEmitter from 'eventemitter3'

import eventRecordToDTO from '../helpers/eventRecordToDTO'

export default function GetEvents (db) {
  return function getEvents ({fromEventId, limit}) {
    let results = new EventEmitter()

    getLastEvent(db)
    .then((lastEvent) => {
      let tillEventId = lastEvent && lastEvent.id
      if (!tillEventId) {
        results.emit('end')
        return
      }

      pullEvents(db, results, 0, limit || Infinity, fromEventId, tillEventId)
      .then(() => results.emit('end'))
      .catch((e) => results.emit('error', e))
    })
    .catch((e) => {
      results.emit('error', e)
    })

    return results
  }
}

function getLastEvent (db) {
  return db.pool.connect()
    .then((client) =>
      client
        .query(`SELECT * FROM ${db.table} ORDER BY id DESC LIMIT 1`)
        .then(({rows}) => {
          client.release()
          return rows[0]
            ? eventRecordToDTO(rows[0])
            : null
        })
        .catch((e) => {
          client.release()
          throw e
        })
    )
}

function pullEvents (db, results, totalEmitted, maxToEmit, fromEventId, tillEventId) {
  return db.pool.connect()
    .then((client) => {
      let c = totalEmitted

      return client.query(
        `SELECT * FROM ${db.table}
          WHERE
            id > $1 AND
            id <= $2
          ORDER BY id LIMIT ${db.batchSize}`,
        [
          fromEventId,
          tillEventId,
        ]
      )
      .then(({rows}) => {
        client.release()
        let toEnd = rows.length < db.batchSize || (c + rows.length) >= maxToEmit
        let lastEmittedId = null
        rows.forEach((row) => {
          if (c < maxToEmit) {
            c++
            results.emit('event', eventRecordToDTO(row))
            lastEmittedId = row.id
          }
        })
        if (toEnd) return null
        return pullEvents(db, results, c, maxToEmit, lastEmittedId, tillEventId)
      })
      .catch((e) => {
        client.release()
        throw e
      })
    })
}
