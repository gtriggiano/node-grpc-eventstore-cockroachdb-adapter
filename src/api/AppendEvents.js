import EventEmitter from 'eventemitter3'
import {
  flatten,
} from 'lodash'
import pickFP from 'lodash/fp/pick'

import eventRecordToDTO from '../helpers/eventRecordToDTO'
import wrapInTransaction from '../helpers/wrapInTransaction'

export const ANY_SEQUENCE_NUMBER = -2
export const ANY_POSITIVE_SEQUENCE_NUMBER = -1

export default function AppendEvents (db) {
  return function appendEvents ({
    appendRequests,
    transactionId,
    correlationId,
  }) {
    let results = new EventEmitter()

    db.pool.connect()
    .then((client) => {
      wrapInTransaction(
        client,
        Execution(db.table, appendRequests, transactionId, correlationId)
      )
      .then((storedEvents) => {
        client.release()
        results.emit('stored-events', storedEvents)
      })
      .catch((e) => {
        client.release()
        results.emit('error', e)
      })
    })
    .catch((e) => {
      results.emit('error', e)
    })

    return results
  }
}

function Execution (table, appendRequests, transactionId, correlationId) {
  return function execution (client) {
    return getStreamsSequenceNumbersForRequests(client, table, appendRequests)
    .then((requestsWithStreamSequenceNumber) => requestsWithStreamSequenceNumber.map(
      ({request, streamSequenceNumber}) => processAppendRequest(request, streamSequenceNumber)
    ))
    .then((processedAppendRequests) => {
      let errors = processedAppendRequests.filter(isError)
      if (errors.length) {
        let msg = JSON.stringify(errors.map(pickFP([
          'message',
          'stream',
          'actualSequenceNumber',
          'expectedSequenceNumber',
        ])))
        throw new Error(`CONSISTENCY|${msg}`)
      }
      return appendEventsToStreams(client, table, transactionId, correlationId, flatten(processedAppendRequests))
    })
  }
}

function getStreamsSequenceNumbersForRequests (client, table, appendRequests) {
  return Promise.all(appendRequests.map((request) =>
    client.query(
      `SELECT max(sequence_number) AS sequence FROM ${table}
        WHERE
          stream_context = $1 AND
          stream_name = $2 AND
          stream_id = $3`,
      [
        request.stream.type.context,
        request.stream.type.name,
        request.stream.id,
      ]
    )
    .then(({rows}) => {
      return {
        request,
        streamSequenceNumber: rows[0].sequence
          ? parseInt(rows[0].sequence, 10)
          : 0,
      }
    })
  ))
}

function processAppendRequest ({stream, events, expectedSequenceNumber}, actualSequenceNumber) {
  if (expectedSequenceNumber !== ANY_SEQUENCE_NUMBER) {
    if (
      actualSequenceNumber === 0 &&
      expectedSequenceNumber === ANY_POSITIVE_SEQUENCE_NUMBER
    ) {
      let error = new Error(`STREAM_DOES_NOT_EXIST`)
      error.stream = stream
      return error
    }
    if (actualSequenceNumber !== expectedSequenceNumber) {
      let error = new Error(`STREAM_SEQUENCE_MISMATCH`)
      error.stream = stream
      error.actualSequenceNumber = actualSequenceNumber
      error.expectedSequenceNumber = expectedSequenceNumber
      return error
    }
  }
  return events.map(({type, data}, idx) => ({
    stream,
    type,
    data,
    sequenceNumber: actualSequenceNumber + idx + 1,
  }))
}

function appendEventsToStreams (client, table, transactionId, correlationId, events) {
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
  let placeholders = events.map(
    (_, eventIdx) =>
      `(${fields.map((_, fieldIdx) => `$${(eventIdx * fields.length) + (fieldIdx + 1)}`).join(',')})`
  ).join(',')

  return client.query(
    `INSERT INTO ${table}
      (${fields.join(',')})
      VALUES
      ${placeholders}
      RETURNING *`,
    events.reduce(
      (placeholders, {stream, type, data, sequenceNumber}) =>
        placeholders.concat(
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
  )
  .then(({rows}) => rows.map(eventRecordToDTO))
}

function isError (e) {
  return e instanceof Error
}
