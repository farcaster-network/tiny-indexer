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
import { bytesToHexString, Message } from '@farcaster/hub-nodejs'
import { HUB_HOST, HUB_SSL, POSTGRES_URL, REDIS_URL } from './env'
import { log } from './log'
import { newQueue } from './worker'
import { ensureMigrations } from './db'
import { startMonitoringServer } from './server'
import backfill from './backfill'
import stream from './stream'
import type { Queue } from 'bullmq'

const hubId = 'shuttle'

export class App implements MessageHandler {
  private static instance: App | null = null
  public redis: RedisClient
  public backfillQueue: Queue
  public hubSubscriber: HubSubscriber
  public streamConsumer: HubEventStreamConsumer
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
    this.backfillQueue = newQueue(redis.client, 'backfill')
  }

  public static async getInstance(
    dbUrl: string,
    redisUrl: string,
    hubUrl: string,
    hubSSL: boolean = false
  ): Promise<App> {
    if (!App.instance) {
      log.info(`Starting app... ${dbUrl}, ${redisUrl}, ${hubUrl}, SSL:${hubSSL}`)

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

      startMonitoringServer()
    }

    return App.instance
  }

  async start() {
    const startedAt = Date.now()
    backfill(this, log)

    const queueIsEmpty =
      (await this.backfillQueue.getActiveCount()) === 0 && (await this.backfillQueue.getWaitingCount()) === 0

    if (queueIsEmpty) {
      log.info(`Backfill completed in ${(Date.now() - startedAt) / 1000}s`)

      stream(this, log)
    }
  }

  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean
  ): Promise<void> {
    // if (!isNew) return
    // if (isCastAddMessage(message) || isCastRemoveMessage(message)) {
    //   if (state === 'created') {
    //     await insertCast(message)
    //   } else if (state === 'deleted') {
    //     await deleteCast(message)
    //   }
    // }
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

await app.start()
