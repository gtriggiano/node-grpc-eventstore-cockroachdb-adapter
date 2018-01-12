import EventEmitter from 'eventemitter3'

import eventRecordToDTO from '../helpers/eventRecordToDTO'

export default function GetEventsByStreamType (db) {
  return function getEventsByStreamType ({streamType, fromEventId, limit}) {
    let results = new EventEmitter()

    getLastEvent(db, streamType)
    .then((lastEvent) => {
      let tillEventId = lastEvent && lastEvent.id
      if (!tillEventId) {
        results.emit('end')
        return
      }

      pullEvents(db, results, 0, limit || Infinity, streamType, fromEventId, tillEventId)
      .then(() => results.emit('end'))
      .catch((e) => results.emit('error', e))
    })
    .catch((e) => {
      results.emit('error', e)
    })

    return results
  }
}

function getLastEvent (db, streamType) {
  let index = `${db.table}_stream_context_stream_name_stream_id_sequence_number_key`
  return db.pool.connect()
    .then((client) =>
      client.query(
        `SELECT * FROM ${db.table}@${index}
          WHERE stream_context = $1 AND stream_name = $2
          ORDER BY id DESC LIMIT 1`,
        [streamType.context, streamType.name]
      )
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

function pullEvents (db, results, totalEmitted, maxToEmit, streamType, fromEventId, tillEventId) {
  let index = `${db.table}_stream_context_stream_name_stream_id_sequence_number_key`
  return db.pool.connect()
    .then((client) => {
      let c = totalEmitted

      return client.query(
        `SELECT * FROM ${db.table}@${index}
          WHERE
            stream_context = $1 AND
            stream_name = $2 AND
            id > $3 AND
            id <= $4
          ORDER BY id LIMIT ${db.batchSize}`,
        [
          streamType.context,
          streamType.name,
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
        return pullEvents(db, results, c, maxToEmit, streamType, lastEmittedId, tillEventId)
      })
      .catch((e) => {
        client.release()
        throw e
      })
    })
}
