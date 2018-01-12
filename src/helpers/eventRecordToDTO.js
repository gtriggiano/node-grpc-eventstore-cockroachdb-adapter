import { pick } from 'lodash'

export default function eventRecordToDTO (record) {
  return {
    ...pick(record, [
      'id',
      'type',
      'data',
    ]),
    stream: {
      id: record.stream_id,
      type: {
        context: record.stream_context,
        name: record.stream_name,
      },
    },
    sequenceNumber: parseInt(record.sequence_number, 10),
    storedOn: record.stored_on.toISOString(),
    transactionId: record.transaction_id,
    correlationId: record.correlation_id || '',
  }
}
