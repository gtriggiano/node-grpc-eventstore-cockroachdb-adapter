import uuid from 'uuid'
import {
  range,
  sample,
  random,
} from 'lodash'

import streams from './streams'

const eventTypes = range(10).map((n) => `SomethingHappened_${n}`)

export default range(2000).map(() => {
  let stream = sample(streams)
  stream.sequence++

  let event = {
    stream: stream,
    type: sample(eventTypes),
    sequenceNumber: stream.sequence,
    data: '',
    transactionId: uuid(),
    correlationId: random(100) > 70 ? uuid() : null,
  }

  stream.events.push(event)
  return event
})
