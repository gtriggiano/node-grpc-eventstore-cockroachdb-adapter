import pool from './connectionPool'

import events from './events'

let fields = [
  'stream_context',
  'stream_name',
  'stream_id',
  'type',
  'sequence_number',
  'data',
  'correlation_id',
  'transaction_id',
]

let query = `INSERT INTO events (${fields.join(',')}) VALUES`
let placeholders = events.map(
  (_, eventIdx) => `(${fields.map((_, fieldIdx) => `$${(fieldIdx + 1) + (eventIdx * fields.length)}`).join(',')})`
)
query = `${query} ${placeholders.join(',')} RETURNING id`

let params = events.reduce(
  (list, {stream, type, sequenceNumber, data, correlationId, transactionId}) => list.concat(
    stream.type.context,
    stream.type.name,
    stream.id,
    type,
    sequenceNumber,
    data,
    correlationId,
    transactionId
  ),
  []
)

export default function populateEventsTable () {
  return pool.connect()
    .then((client) =>
      client
      .query(query, params)
      .then(({rows}) => {
        client.release()
        events.forEach((event, idx) => {
          event.id = rows[idx].id
        })
      })
      .catch((e) => {
        client.release()
        throw e
      })
    )
}
