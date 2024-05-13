import {
  RedisClient,
  getHubClient,
  EventStreamConnection,
  EventStreamHubSubscriber,
  type MessageHandler,
  type DB,
  HubSubscriber,
  HubEventProcessor,
  HubEventStreamConsumer,
  MessageReconciliation,
  getDbClient,
  type StoreMessageOperation,
  type MessageState,
} from '@farcaster/shuttle'
import { bytesToHexString, Message, isCastAddMessage, isCastRemoveMessage } from '@farcaster/hub-nodejs'
import { BACKFILL_FIDS, CONCURRENCY, HUB_HOST, HUB_SSL, MAX_FID, POSTGRES_URL, REDIS_URL } from './env'
import { log } from './log'
import { ok } from 'neverthrow'
import { newQueue, newWorker } from './worker'
import { ensureMigrations } from './db'
import { deleteCast, insertCast } from '../lib/casts'
import type { Queue } from 'bullmq'
import { startServer } from './server'

const hubId = 'shuttle'

export class App implements MessageHandler {
  private static instance: App | null = null
  public redis: RedisClient
  // public backfillQueue: Queue
  private hubSubscriber: HubSubscriber
  private streamConsumer: HubEventStreamConsumer
  public readonly db: DB
  private readonly hubId: string

  private constructor(
    db: DB,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer
  ) {
    this.db = db
    this.redis = redis
    this.hubSubscriber = hubSubscriber
    this.hubId = hubId
    this.streamConsumer = streamConsumer
    // this.backfillQueue = newQueue(redis.client, 'backfill')
  }

  public static async getInstance(
    dbUrl: string,
    redisUrl: string,
    hubUrl: string,
    hubSSL: boolean = false
  ): Promise<App> {
    if (!App.instance) {
      const [db, redis, hub] = await Promise.all([
        getDbClient(dbUrl),
        RedisClient.create(redisUrl),
        getHubClient(hubUrl, { ssl: hubSSL }),
      ])

      const eventStreamForWrite = new EventStreamConnection(redis.client)
      const eventStreamForRead = new EventStreamConnection(redis.client)
      const hubSubscriber = new EventStreamHubSubscriber('shuttle', hub, eventStreamForWrite, redis, 'all', log)
      const streamConsumer = new HubEventStreamConsumer(hub, eventStreamForRead, 'all')

      App.instance = new App(db, redis, hubSubscriber, streamConsumer)

      await ensureMigrations(App.instance.db, log)
      startServer()
    }
    return App.instance
  }

  async stream() {
    log.info(`Starting stream... ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}, SSL:${HUB_SSL}`)

    // Hub subscriber listens to events from the hub and writes them to a redis stream.
    await this.hubSubscriber.start()

    // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
    // TODO: there has to be a better way to do this
    await new Promise((resolve) => setTimeout(resolve, 10_000))

    // Stream consumer reads from the redis stream and inserts them into postgres
    await this.streamConsumer.start(async (hubEvent) => {
      await HubEventProcessor.processHubEvent(this.db, hubEvent, this)
      return ok({ skipped: false })
    })
  }

  // async backfill() {
  //   log.info(`Starting backfill... ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}, SSL:${HUB_SSL}`)
  //   const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(',').map((fid) => parseInt(fid)) : []

  //   const startedAt = Date.now()
  //   if (fids.length === 0) {
  //     const maxFidResult = await this.hubSubscriber.hubClient!.getFids({
  //       pageSize: 1,
  //       reverse: true,
  //     })
  //     if (maxFidResult.isErr()) {
  //       log.error('Failed to get max fid', maxFidResult.error)
  //       throw maxFidResult.error
  //     }
  //     const maxFid = MAX_FID ? parseInt(MAX_FID) : maxFidResult.value.fids[0]
  //     if (!maxFid) {
  //       log.error('Max fid was undefined')
  //       throw new Error('Max fid was undefined')
  //     }
  //     log.info(`Queuing up fids upto: ${maxFid}`)
  //     // create an array of arrays in batches of 100 up to maxFid
  //     const batchSize = 10
  //     const fids = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize).map((fid) => fid + 1)
  //     for (const start of fids) {
  //       const subset = Array.from({ length: batchSize }, (_, i) => start + i)
  //       await this.backfillQueue.add('reconcile', { fids: subset })
  //     }
  //   } else {
  //     await this.backfillQueue.add('reconcile', { fids })
  //   }

  //   await this.backfillQueue.add('completionMarker', { startedAt })
  //   log.info('Backfill jobs queued')

  //   log.info(`Starting worker...`)
  //   await newWorker(this, app.redis.client, log, CONCURRENCY).run()
  //   return
  // }

  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean
  ): Promise<void> {
    if (!isNew) return

    if (isCastAddMessage(message) || isCastRemoveMessage(message)) {
      if (state === 'created') {
        await insertCast(message)
      } else if (state === 'deleted') {
        await deleteCast(message)
      }
    }

    const description = wasMissed ? `missed message (${operation})` : `message (${operation})`
    log.info(`${state} ${description} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${message.data?.type})`)
  }

  async reconcileFids(fids: number[]) {
    const reconciler = new MessageReconciliation(this.hubSubscriber.hubClient!, this.db, log)
    for (const fid of fids) {
      await reconciler.reconcileMessagesForFid(fid, async (message, missingInDb, prunedInDb, revokedInDb) => {
        if (missingInDb) {
          await HubEventProcessor.handleMissingMessage(this.db, message, this)
        } else if (prunedInDb || revokedInDb) {
          const messageDesc = prunedInDb ? 'pruned' : revokedInDb ? 'revoked' : 'existing'
          log.info(`Reconciled ${messageDesc} message ${bytesToHexString(message.hash)._unsafeUnwrap()}`)
        }
      })
    }
  }

  async stop() {
    this.hubSubscriber.stop()
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId)
    log.info(`Stopped at eventId: ${lastEventId}`)
  }
}

const app = await App.getInstance(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL)

// await app.backfill()
await app.stream()
