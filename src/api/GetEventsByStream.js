import EventEmitter from 'eventemitter3'

import eventRecordToDTO from '../helpers/eventRecordToDTO'

export default function GetEventsByStream (db) {
  return function getEventsByStream ({stream, fromSequenceNumber, limit}) {
    let results = new EventEmitter()

    getLastEvent(db, stream)
    .then((lastEvent) => {
      let tillEventId = lastEvent && lastEvent.id
      if (!tillEventId) {
        results.emit('end')
        return
      }

      pullEvents(db, results, 0, limit || Infinity, stream, fromSequenceNumber, tillEventId)
      .then(() => results.emit('end'))
      .catch((e) => results.emit('error', e))
    })
    .catch((e) => {
      results.emit('error', e)
    })

    return results
  }
}

function getLastEvent (db, stream) {
  let index = `${db.table}_stream_context_stream_name_stream_id_sequence_number_key`
  return db.pool.connect()
    .then((client) =>
      client.query(
        `SELECT * FROM ${db.table}@${index}
          WHERE stream_context = $1 AND stream_name = $2 AND stream_id = $3
          ORDER BY id DESC LIMIT 1`,
        [stream.type.context, stream.type.name, stream.id]
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

function pullEvents (db, results, totalEmitted, maxToEmit, stream, fromSequenceNumber, tillEventId) {
  let index = `${db.table}_stream_context_stream_name_stream_id_sequence_number_key`
  return db.pool.connect()
    .then((client) => {
      let c = totalEmitted

      return client.query(
        `SELECT * FROM ${db.table}@${index}
          WHERE
            stream_context = $1 AND
            stream_name = $2 AND
            stream_id = $3 AND
            sequence_number > $4 AND
            id <= $5
          ORDER BY id LIMIT ${db.batchSize}`,
        [
          stream.type.context,
          stream.type.name,
          stream.id,
          fromSequenceNumber,
          tillEventId,
        ]
      )
      .then(({rows}) => {
        client.release()
        let toEnd = rows.length < db.batchSize || (c + rows.length) >= maxToEmit
        let lastEmittedSequenceNumber = null
        rows.forEach((row) => {
          if (c < maxToEmit) {
            c++
            let event = eventRecordToDTO(row)
            results.emit('event', event)
            lastEmittedSequenceNumber = event.sequenceNumber
          }
        })
        if (toEnd) return null
        return pullEvents(db, results, c, maxToEmit, stream, lastEmittedSequenceNumber, tillEventId)
      })
      .catch((e) => {
        client.release()
        throw e
      })
    })
}
