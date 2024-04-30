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
} from "@farcaster/shuttle";
import { bytesToHexString } from "@farcaster/hub-nodejs";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  MAX_FID,
  POSTGRES_URL,
  REDIS_URL,
} from "./env";
import { log } from "./log";
import { ok } from "neverthrow";
import { getQueue, getWorker } from "./worker";
import { ensureMigrations } from "./db";
import { addProgressBar } from "./utils/progressBar";
import type { SingleBar } from "cli-progress";

const hubId = "shuttle";

export class App implements MessageHandler {
  private hubSubscriber: HubSubscriber;
  private streamConsumer: HubEventStreamConsumer;
  public readonly db: DB;
  public redis: RedisClient;
  private readonly hubId;

  constructor(
    db: DB,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer
  ) {
    this.db = db;
    this.redis = redis;
    this.hubSubscriber = hubSubscriber;
    this.hubId = hubId;
    this.streamConsumer = streamConsumer;
  }

  static create(
    dbUrl: string,
    redisUrl: string,
    hubUrl: string,
    hubSSL = false
  ) {
    const db = getDbClient(dbUrl);
    const hub = getHubClient(hubUrl, { ssl: hubSSL });
    const redis = RedisClient.create(redisUrl);
    const eventStreamForWrite = new EventStreamConnection(redis.client);
    const eventStreamForRead = new EventStreamConnection(redis.client);
    const hubSubscriber = new EventStreamHubSubscriber(
      hubId,
      hub,
      eventStreamForWrite,
      redis,
      "all",
      log
    );
    const streamConsumer = new HubEventStreamConsumer(
      hub,
      eventStreamForRead,
      "all"
    );

    return new App(db, redis, hubSubscriber, streamConsumer);
  }

  async stream() {
    // Hub subscriber listens to events from the hub and writes them to a redis stream. This allows for scaling by
    // splitting events to multiple streams
    await this.hubSubscriber.start();

    // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
    // TODO: there has to be a better way to do this
    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("Starting stream consumer");
    // Stream consumer reads from the redis stream and inserts them into postgres
    await this.streamConsumer.start(async (hubEvent) => {
      await HubEventProcessor.processHubEvent(this.db, hubEvent, this);
      return ok({ skipped: false });
    });
  }

  async backfill() {
    const fids = BACKFILL_FIDS
      ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid))
      : [];

    const backfillQueue = getQueue(this.redis.client);

    const startedAt = Date.now();
    if (fids.length === 0) {
      const maxFidResult = await this.hubSubscriber.hubClient.getFids({
        pageSize: 1,
        reverse: true,
      });
      if (maxFidResult.isErr()) {
        log.error("Failed to get max fid", maxFidResult.error);
        throw maxFidResult.error;
      }
      const maxFid = MAX_FID ? parseInt(MAX_FID) : maxFidResult.value.fids[0];
      if (!maxFid) {
        log.error("Max fid was undefined");
        throw new Error("Max fid was undefined");
      }
      log.info(`Queuing up fids upto: ${maxFid}`);
      // create an array of arrays in batches of 100 up to maxFid
      const batchSize = 10;
      const fids = Array.from(
        { length: Math.ceil(maxFid / batchSize) },
        (_, i) => i * batchSize
      ).map((fid) => fid + 1);
      for (const start of fids) {
        const subset = Array.from({ length: batchSize }, (_, i) => start + i);
        await backfillQueue.add("reconcile", { fids: subset });
      }
    } else {
      await backfillQueue.add("reconcile", { fids });
    }

    await backfillQueue.add("completionMarker", { startedAt });
    log.info("Backfill jobs queued");

    log.info(`Starting worker...`);
    const worker = getWorker(this, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  async handleMessageMerge(): Promise<void> {}

  async reconcileFids(fids: number[]) {
    // biome-ignore lint/style/noNonNullAssertion: client is always initialized
    const reconciler = new MessageReconciliation(
      this.hubSubscriber.hubClient!,
      this.db,
      log
    );
    for (const fid of fids) {
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          if (missingInDb) {
            await HubEventProcessor.handleMissingMessage(
              this.db,
              message,
              this
            );
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb
              ? "pruned"
              : revokedInDb
              ? "revoked"
              : "existing";
            log.info(
              `Reconciled ${messageDesc} message ${bytesToHexString(
                message.hash
              )._unsafeUnwrap()}`
            );
          }
        }
      );
    }
  }

  async stop() {
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`Stopped at eventId: ${lastEventId}`);
  }
}

const app = App.create(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL);

ensureMigrations(app.db, log);

const startTime = Date.now();

log.info(
  `Starting backfill... ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}, SSL:${HUB_SSL}`
);
app.backfill();

const endTime = Date.now();

log.info(`Backfill took ${endTime - startTime}ms`);
