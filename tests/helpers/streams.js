import uuid from 'uuid'
import {
  range,
  sample,
} from 'lodash'

const contexts = range(5).map((n) => `Context_${n}`)
const streams = range(10).map((n) => ({
  id: uuid(),
  type: {
    context: sample(contexts),
    name: `Stream_${n}`,
  },
  sequence: 0,
  events: [],
}))

export default streams
export const streamSerials = streams.map(({id, type: {context, name}}) => `${context}:${name}:${id}`)
