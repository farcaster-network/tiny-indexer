import { App } from './app'
import { pino } from 'pino'
import { ok } from 'neverthrow'
import { HubEventProcessor } from '@farcaster/shuttle'

export default async function stream(app: App, log: pino.Logger) {
  log.info('Starting stream...')

  // Hub subscriber listens to events from the hub and writes them to a redis stream.
  await app.hubSubscriber.start()

  // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
  // TODO: there has to be a better way to do this
  await new Promise((resolve) => setTimeout(resolve, 10_000))

  // Stream consumer reads from the redis stream and inserts them into postgres
  await app.streamConsumer.start(async (hubEvent) => {
    await HubEventProcessor.processHubEvent(app.db, hubEvent, app)
    return ok({ skipped: false })
  })
}
