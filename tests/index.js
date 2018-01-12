/* global describe it before beforeEach */
import path from 'path'
import should from 'should/as-function'
import EventEmitter from 'eventemitter3'
import uuid from 'uuid'
import {
  shuffle,
  random,
  last,
  pick,
  sample,
  every,
  isString,
} from 'lodash'

import streams from './helpers/streams'
import events from './helpers/events'
import createEventsTable from './helpers/createEventsTable'
import getEventsCount from './helpers/getEventsCount'
import getStreams from './helpers/getStreams'
import truncateEventsTable from './helpers/truncateEventsTable'
import populateEventsTable from './helpers/populateEventsTable'

const codeFolder = path.resolve(__dirname, '..', process.env.CODE_FOLDER)
const { default: CockroachDBAdapter, createTableSQL } = require(codeFolder)

let getAdapter = () => CockroachDBAdapter({host: 'cockroachdb'})

describe('createTableSQL([tableName])', () => {
  it('is a function', () => {
    should(createTableSQL).be.a.Function()
  })
  it('returns a `CREATE TABLE` sql query', () => {
    let defaultSQL = createTableSQL()
    let customSQL = createTableSQL('mytable')

    should(defaultSQL).be.a.String()
    should(defaultSQL.indexOf('CREATE TABLE IF NOT EXISTS events')).equal(0)
    should(customSQL).be.a.String()
    should(customSQL.indexOf('CREATE TABLE IF NOT EXISTS mytable')).equal(0)
  })
})

describe('CockroachDBAdapter([config])', () => {
  it('is a function', () => {
    should(CockroachDBAdapter).be.a.Function()
  })
  it('passing `config` is optional', () => {
    should(() => {
      CockroachDBAdapter()
    }).not.throw()
  })
  it('throws if config.host is not a non empty string', () => {
    should(() => {
      CockroachDBAdapter({host: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({host: 3})
    }).throw()
    should(() => {
      CockroachDBAdapter({host: 'test'})
    }).not.throw()
  })
  it('throws if config.port is not a valid port', () => {
    should(() => {
      CockroachDBAdapter({port: 0})
    }).throw()
    should(() => {
      CockroachDBAdapter({port: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({port: 1000})
    }).not.throw()
  })
  it('throws if config.database is not a non empty string', () => {
    should(() => {
      CockroachDBAdapter({database: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({database: 3})
    }).throw()
    should(() => {
      CockroachDBAdapter({database: 'test'})
    }).not.throw()
  })
  it('throws if config.table is not a non empty string', () => {
    should(() => {
      CockroachDBAdapter({table: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({table: 3})
    }).throw()
    should(() => {
      CockroachDBAdapter({table: 'test'})
    }).not.throw()
  })
  it('throws if config.user is not a non empty string', () => {
    should(() => {
      CockroachDBAdapter({user: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({user: 3})
    }).throw()
    should(() => {
      CockroachDBAdapter({user: 'test'})
    }).not.throw()
  })
  it('throws if config.maxPoolClients is not a positive integer', () => {
    should(() => {
      CockroachDBAdapter({maxPoolClients: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({maxPoolClients: 0})
    }).throw()
    should(() => {
      CockroachDBAdapter({maxPoolClients: 10})
    }).not.throw()
  })
  it('throws if config.idleTimeoutMillis is not an integer >= 1000', () => {
    should(() => {
      CockroachDBAdapter({idleTimeoutMillis: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({idleTimeoutMillis: 100})
    }).throw()
    should(() => {
      CockroachDBAdapter({idleTimeoutMillis: 1000})
    }).not.throw()
  })
  it('throws if config.batchSize is not an integer >= 50', () => {
    should(() => {
      CockroachDBAdapter({batchSize: ''})
    }).throw()
    should(() => {
      CockroachDBAdapter({batchSize: 49})
    }).throw()
    should(() => {
      CockroachDBAdapter({batchSize: 50})
    }).not.throw()
  })
  it('throws if config.ssl is not valid', () => {
    should(() => {
      CockroachDBAdapter({ssl: 4})
    }).throw()
    should(() => {
      CockroachDBAdapter({ssl: {}})
    }).throw()
    should(() => {
      const ssl = {
        ca: 'xyz',
        cert: 'xyz',
        key: 'xyz',
      }

      const badSSLConfig = shuffle(Object.keys(ssl)).slice(1).reduce((map, key) => ({...map, [key]: ssl[key]}), {})
      CockroachDBAdapter({ssl: badSSLConfig})
    }).throw()
    should(() => {
      CockroachDBAdapter({
        ssl: {
          ca: 'xyz',
          cert: 'xyz',
          key: 'xyz',
        },
      })
    }).not.throw()
  })
})

describe('db = CockroachDBAdapter()', () => {
  it('db.getEvents() is a function', () => should(CockroachDBAdapter().getEvents).be.a.Function())
  it('db.getEventsByStream() is a function', () => should(CockroachDBAdapter().getEventsByStream).be.a.Function())
  it('db.getEventsByStreamType() is a function', () => should(CockroachDBAdapter().getEventsByStreamType).be.a.Function())
  it('db.appendEvents() is a function', () => should(CockroachDBAdapter().appendEvents).be.a.Function())
})

describe('results = db.getEvents({fromEventId[, limit]})', () => {
  before(() => createEventsTable())
  beforeEach(() => truncateEventsTable().then(() => populateEventsTable()))

  it('is an event emitter', (done) => {
    let db = getAdapter()
    let results = db.getEvents({fromEventId: last(events).id})
    should(results).be.an.instanceOf(EventEmitter)
    results.on('error', done)
    results.on('end', () => done())
  })
  it('emits just events with `id` > `fromEventId`. Emits `event` for each event fetched and then emits `end`. Events are ordered by id ASC', (done) => {
    let db = getAdapter()
    let sampleEventIdx = random(0, events.length - 1)
    let sampleEvent = events[sampleEventIdx]
    let expectedEventIds = events.slice(sampleEventIdx + 1).map(({id}) => id)
    let results = db.getEvents({fromEventId: sampleEvent.id})

    let fetchedEventIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('for every event emitted the .correlationId is always a string', (done) => {
    let db = getAdapter()
    let sampleEventIdx = random(0, events.length - 1)
    let sampleEvent = events[sampleEventIdx]
    let results = db.getEvents({fromEventId: sampleEvent.id})

    let fetchedEventCorrelationIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventCorrelationIds.push(event.correlationId))
    results.on('end', () => {
      try {
        should(every(fetchedEventCorrelationIds, isString)).be.True()
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('emits just `end` if no events are found', (done) => {
    let db = getAdapter()
    let results = db.getEvents({fromEventId: last(events).id})
    results.on('error', done)
    results.on('event', () => done(new Error()))
    results.on('end', () => done())
  })
  it('takes in to account `limit` param if provided', (done) => {
    let db = getAdapter()
    let sampleEventIdx = random(0, events.length - 3)
    let sampleEvent = events[sampleEventIdx]
    let remainingEvents = events.slice(sampleEventIdx + 1)
    let limit = random(1, remainingEvents.length)
    let expectedEventIds = remainingEvents.slice(0, limit).map(({id}) => id)

    let results = db.getEvents({fromEventId: sampleEvent.id, limit: limit})

    let fetchedEventIds = []
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})

describe('results = db.getEventsByStream({stream, fromSequenceNumber[, limit]})', () => {
  before(() => createEventsTable())
  beforeEach(() => truncateEventsTable().then(() => populateEventsTable()))

  it('is an event emitter', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let results = db.getEventsByStream({
      stream: pick(stream, ['id', 'type']),
      fromSequenceNumber: 300,
    })
    should(results).be.an.instanceOf(EventEmitter)
    results.on('error', done)
    results.on('end', () => done())
  })
  it('emits just events belonging to the stream, with `sequenceNumber` > `fromSequenceNumber`. Emits `event` for each event fetched and then emits `end`. Events are ordered by id ASC', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let fromSequenceNumber = random(0, stream.sequence)
    let expectedEventIds = stream.events.filter(({sequenceNumber}) => sequenceNumber > fromSequenceNumber).map(({id}) => id)

    let results = db.getEventsByStream({
      stream: pick(stream, ['id', 'type']),
      fromSequenceNumber: fromSequenceNumber,
    })

    let fetchedEventIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('for every event emitted the .correlationId is always a string', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let fromSequenceNumber = random(0, stream.sequence)

    let results = db.getEventsByStream({
      stream: pick(stream, ['id', 'type']),
      fromSequenceNumber: fromSequenceNumber,
    })

    let fetchedEventCorrelationIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventCorrelationIds.push(event.correlationId))
    results.on('end', () => {
      try {
        should(every(fetchedEventCorrelationIds, isString)).be.True()
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('emits just `end` if no events are found', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let fromSequenceNumber = stream.sequence

    let results = db.getEventsByStream({
      stream: pick(stream, ['id', 'type']),
      fromSequenceNumber: fromSequenceNumber,
    })

    results.on('error', done)
    results.on('event', () => done(new Error('should not fine any event')))
    results.on('end', () => done())
  })
  it('takes in to account `limit` param if provided', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let fromSequenceNumber = random(0, stream.sequence - 2)
    let expectedEventIds = stream.events.filter(({sequenceNumber}) => sequenceNumber > fromSequenceNumber).map(({id}) => id)
    let limit = random(1, expectedEventIds.length)
    expectedEventIds = expectedEventIds.slice(0, limit)

    let results = db.getEventsByStream({
      stream: pick(stream, ['id', 'type']),
      fromSequenceNumber: fromSequenceNumber,
      limit: limit,
    })

    let fetchedEventIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})

describe('results = db.getEventsByStreamType({streamType, fromEventId[, limit]})', () => {
  before(() => createEventsTable())
  beforeEach(() => truncateEventsTable().then(() => populateEventsTable()))

  it('is an event emitter', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let results = db.getEventsByStreamType({
      streamType: stream.type,
      fromEventId: last(events.id),
    })
    should(results).be.an.instanceOf(EventEmitter)
    results.on('error', done)
    results.on('end', () => done())
  })
  it('emits just events belonging to streams of type, with `id` > `fromEventId`. Emits `event` for each event fetched and then emits `end`. Events are ordered by id ASC', (done) => {
    let db = getAdapter()
    let streamType = sample(streams).type
    let eventsOfType = events.filter(({stream: {type}}) => type.context === streamType.context && type.name === streamType.name)
    let sampleEventIdx = random(0, eventsOfType.length - 1)
    let sampleEvent = eventsOfType[sampleEventIdx]
    let expectedEventIds = eventsOfType.slice(sampleEventIdx + 1).map(({id}) => id)

    let results = db.getEventsByStreamType({
      streamType: streamType,
      fromEventId: sampleEvent.id,
    })

    let fetchedEventIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('for every event emitted the .correlationId is always a string', (done) => {
    let db = getAdapter()
    let streamType = sample(streams).type
    let eventsOfType = events.filter(({stream: {type}}) => type.context === streamType.context && type.name === streamType.name)
    let sampleEventIdx = random(0, eventsOfType.length - 1)
    let sampleEvent = eventsOfType[sampleEventIdx]

    let results = db.getEventsByStreamType({
      streamType: streamType,
      fromEventId: sampleEvent.id,
    })

    let fetchedEventCorrelationIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventCorrelationIds.push(event.correlationId))
    results.on('end', () => {
      try {
        should(every(fetchedEventCorrelationIds, isString)).be.True()
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('emits just `end` if no events are found', (done) => {
    let db = getAdapter()
    let streamType = sample(streams).type
    let eventsOfType = events.filter(({stream: {type}}) => type.context === streamType.context && type.name === streamType.name)

    let results = db.getEventsByStreamType({
      streamType: streamType,
      fromEventId: last(eventsOfType).id,
    })

    results.on('error', done)
    results.on('event', () => done(new Error('should not fine any event')))
    results.on('end', () => done())
  })
  it('takes in to account `limit` param if provided', (done) => {
    let db = getAdapter()
    let streamType = sample(streams).type
    let eventsOfType = events.filter(({stream: {type}}) => type.context === streamType.context && type.name === streamType.name)
    let sampleEventIdx = random(0, eventsOfType.length - 3)
    let sampleEvent = eventsOfType[sampleEventIdx]
    let expectedEventIds = eventsOfType.slice(sampleEventIdx + 1).map(({id}) => id)
    let limit = random(1, expectedEventIds.length)
    expectedEventIds = expectedEventIds.slice(0, limit)

    let results = db.getEventsByStreamType({
      streamType: streamType,
      fromEventId: sampleEvent.id,
      limit: limit,
    })

    let fetchedEventIds = []
    results.on('error', done)
    results.on('event', (event) => fetchedEventIds.push(event.id))
    results.on('end', () => {
      try {
        should(fetchedEventIds).eql(expectedEventIds)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})

describe('results = db.appendEvents({appendRequests, transactionId, correlationId})', () => {
  before(() => createEventsTable())
  beforeEach(() => truncateEventsTable().then(() => populateEventsTable()))

  it('is an event emitter', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let results = db.appendEvents({
      appendRequests: [
        {
          stream: pick(stream, ['id', 'type']),
          events: [
            {
              type: 'aType',
              data: '',
            },
          ],
          expectedSequenceNumber: stream.sequence,
        },
      ],
      transactionId: uuid(),
      correlationId: uuid(),
    })
    should(results).be.an.instanceOf(EventEmitter)
    results.on('error', done)
    results.on('stored-events', () => done())
  })
  it('emits `stored-events` with a list of created events, ordered by id ASC', (done) => {
    let db = getAdapter()
    let stream = sample(streams)

    getEventsCount()
    .then((totalInitialEvents) => {
      let newEvent1 = {
        type: uuid(),
        data: uuid(),
      }
      let newEvent2 = {
        type: uuid(),
        data: uuid(),
      }
      let results = db.appendEvents({
        appendRequests: [
          {
            stream: pick(stream, ['id', 'type']),
            events: [
              newEvent1,
              newEvent2,
            ],
            expectedSequenceNumber: stream.sequence,
          },
        ],
        transactionId: uuid(),
        correlationId: uuid(),
      })
      results.on('error', done)
      results.on('stored-events', (newEvents) => {
        getEventsCount()
        .then((totalFinalEvents) => {
          try {
            should(totalFinalEvents).equal(totalInitialEvents + 2)
            should(newEvents.length).equal(2)
            should(newEvents[0].type === newEvent1.type)
            should(newEvents[0].data === newEvent1.data)
            should(newEvents[1].type === newEvent2.type)
            should(newEvents[1].data === newEvent2.data)
            should(newEvents[1].id > newEvents[0].id)
            done()
          } catch (e) {
            done(e)
          }
        })
        .catch(done)
      })
    })
    .catch(done)
  })
  it('for every event carried in the `stored-events` event payolad .correlationId is always a string', (done) => {
    let db = getAdapter()
    let stream = sample(streams)

    let results = db.appendEvents({
      appendRequests: [
        {
          stream: pick(stream, ['id', 'type']),
          events: [
            {type: 'aType', data: ''},
            {type: 'aType', data: ''},
          ],
          expectedSequenceNumber: stream.sequence,
        },
      ],
      transactionId: uuid(),
      correlationId: null,
    })

    results.on('stored-events', (newEvents) => {
      try {
        should(every(newEvents.map(({correlationId}) => correlationId), isString)).be.True()
        done()
      } catch (e) {
        done(e)
      }
    })
  })
  it('emits `error` if there is a sequence mismatch', (done) => {
    let db = getAdapter()
    let stream = sample(streams)
    let streamVersion = stream.sequence
    stream = pick(stream, ['id', 'type'])

    let results = db.appendEvents({
      appendRequests: [
        {
          stream: stream,
          events: [],
          expectedSequenceNumber: streamVersion - 1,
        },
      ],
      transactionId: uuid(),
    })

    results.on('error', (err) => {
      try {
        should(err.message.indexOf('CONSISTENCY|')).equal(0)
        let jsonErrors = JSON.parse(err.message.split('|')[1])
        should(jsonErrors).containDeepOrdered([
          {
            message: 'STREAM_SEQUENCE_MISMATCH',
            stream: stream,
            actualSequenceNumber: streamVersion,
            expectedSequenceNumber: streamVersion - 1,
          },
        ])
        done()
      } catch (e) {
        done(e)
      }
    })
    results.on('stored-events', () => done(new Error('should emit error')))
  })
  it('emits `error` if expectedSequenceNumber === -1 and the stream does not exist', (done) => {
    let db = getAdapter()
    let transactionId = uuid()
    let correlationId = random(100) > 50 ? uuid() : null

    let stream = {
      ...pick(sample(streams), ['type']),
      id: 'notExistent',
    }

    let results = db.appendEvents({
      appendRequests: [
        {
          stream: stream,
          events: [
            {type: 'aType', data: ''},
          ],
          expectedSequenceNumber: -1,
        },
      ],
      transactionId,
      correlationId,
    })

    results.on('error', (err) => {
      try {
        should(err.message.indexOf('CONSISTENCY|')).equal(0)
        let jsonErrors = JSON.parse(err.message.split('|')[1])
        should(jsonErrors).containDeepOrdered([
          {
            message: 'STREAM_DOES_NOT_EXIST',
            stream: stream,
          },
        ])
        done()
      } catch (e) {
        done(e)
      }
    })
    results.on('stored-events', () => done(new Error('should emit error')))
  })
  it('DOES NOT write any event if the writing to any stream fails', (done) => {
    let db = getAdapter()
    let transactionId = uuid()
    let correlationId = null

    let stream = sample(streams)
    let streamVersion = stream.sequence
    stream = pick(stream, ['id', 'type'])
    let newStream = {
      id: uuid(),
      type: {
        context: uuid(),
        name: uuid(),
      },
    }

    let results = db.appendEvents({
      appendRequests: [
        {
          stream: stream,
          events: [
            {type: 'aType', data: ''},
            {type: 'aType', data: ''},
          ],
          expectedSequenceNumber: streamVersion - 1,
        },
        {
          stream: newStream,
          events: [
            {type: 'aType', data: ''},
          ],
          expectedSequenceNumber: 0,
        },
      ],
      transactionId,
      correlationId,
    })

    results.on('error', () => {
      getEventsCount()
      .then((totalEvents) => {
        should(totalEvents).equal(events.length)
        done()
      })
      .catch(done)
    })
    results.on('stored-events', () => done(new Error('should not emit stored events')))
  })
  it('appends the events if expectedSequenceNumber === -2', (done) => {
    let db = getAdapter()
    let stream = sample(streams)

    getEventsCount()
    .then((totalInitialEvents) => {
      let newEvent1 = {
        type: uuid(),
        data: uuid(),
      }
      let newEvent2 = {
        type: uuid(),
        data: uuid(),
      }
      let results = db.appendEvents({
        appendRequests: [
          {
            stream: pick(stream, ['id', 'type']),
            events: [
              newEvent1,
              newEvent2,
            ],
            expectedSequenceNumber: -2,
          },
        ],
        transactionId: uuid(),
        correlationId: uuid(),
      })
      results.on('error', done)
      results.on('stored-events', (newEvents) => {
        getEventsCount()
        .then((totalFinalEvents) => {
          try {
            should(totalFinalEvents).equal(totalInitialEvents + 2)
            should(newEvents.length).equal(2)
            should(newEvents[0].type === newEvent1.type)
            should(newEvents[0].data === newEvent1.data)
            should(newEvents[1].type === newEvent2.type)
            should(newEvents[1].data === newEvent2.data)
            should(newEvents[1].id > newEvents[0].id)
            done()
          } catch (e) {
            done(e)
          }
        })
        .catch(done)
      })
    })
    .catch(done)
  })
  it('creates a new stream if writing to a not existent stream with expectedSequenceNumber === -2 or 0', (done) => {
    let db = getAdapter()
    let transactionId = uuid()
    let correlationId = uuid()

    let newStream = {
      id: uuid(),
      type: {
        context: uuid(),
        name: uuid(),
      },
    }
    let newStream2 = {
      id: uuid(),
      type: {
        context: uuid(),
        name: uuid(),
      },
    }

    getStreams()
    .then((initialStreams) =>
      getEventsCount()
      .then((totalInitialEvents) => {
        let results = db.appendEvents({
          appendRequests: [
            {
              stream: newStream,
              events: [
                {type: 'aType', data: ''},
              ],
              expectedSequenceNumber: -2,
            },
            {
              stream: newStream2,
              events: [
                {type: 'aType', data: ''},
              ],
              expectedSequenceNumber: 0,
            },
          ],
          transactionId,
          correlationId,
        })

        results.on('error', done)
        results.on('stored-events', () => {
          getStreams()
          .then((finalStreams) =>
            getEventsCount()
            .then((totalFinalEvents) => {
              should(totalFinalEvents).equal(totalInitialEvents + 2)
              should(finalStreams.length).equal(initialStreams.length + 2)
              should(finalStreams).containEql(newStream)
              should(finalStreams).containEql(newStream2)
              done()
            })
            .catch(done)
          )
        })
      })
    )
    .catch(done)
  })
  it('saves events for multiple streams within the same transaction', (done) => {
    let db = getAdapter()
    let newStream = {
      id: uuid(),
      type: {
        context: uuid(),
        name: uuid(),
      },
    }
    let newStream2 = {
      id: uuid(),
      type: {
        context: uuid(),
        name: uuid(),
      },
    }

    let results = db.appendEvents({
      appendRequests: [
        {
          stream: newStream,
          events: [
            {
              type: 'aType',
              data: '',
            },
          ],
          expectedSequenceNumber: 0,
        },
        {
          stream: newStream2,
          events: [
            {
              type: 'aType',
              data: '',
            },
          ],
          expectedSequenceNumber: 0,
        },
      ],
      transactionId: uuid(),
      correlationId: uuid(),
    })

    results.on('error', done)
    results.on('stored-events', (storedEvents) => {
      try {
        should(storedEvents.length).equal(2)
        should(storedEvents[0].storedOn).equal(storedEvents[1].storedOn)
        done()
      } catch (e) {
        done(e)
      }
    })
  })
})
