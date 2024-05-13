import {
  Message,
  type CastAddMessage,
  type CastRemoveMessage,
  isCastAddMessage,
  isCastRemoveMessage,
} from '@farcaster/hub-nodejs'
import { farcasterTimeToDate } from '../src/utils.js'
import type { Insertable } from 'kysely'
import { db, type Tables } from '../src/db'
import { log } from '../src/log.js'

export async function insertCast(msg: CastAddMessage | CastRemoveMessage) {
  if (!isCastAddMessage(msg)) {
    throw new Error('Invalid message')
  }

  const cast = formatCastMessage(msg)

  try {
    await db
      .insertInto('casts')
      .values(cast)
      .onConflict((oc) => oc.column('hash').doNothing())
      .execute()

    log.debug(`CAST INSERTED`)
  } catch (error) {
    log.error(error, 'ERROR INSERTING CAST')
    throw error
  }
}

export async function deleteCast(msg: CastAddMessage | CastRemoveMessage) {
  try {
    await db
      .updateTable('casts')
      .set({ deletedAt: farcasterTimeToDate(msg.data.timestamp) || new Date() })
      .where('hash', '=', msg.hash)
      .execute()

    log.debug(`CAST DELETED`)
  } catch (error) {
    log.error(error, 'ERROR DELETING CAST')
    throw error
  }
}

function formatCastMessage(msg: CastAddMessage) {
  const data = msg.data
  const castAddBody = data.castAddBody

  return {
    timestamp: farcasterTimeToDate(data.timestamp),
    fid: data.fid,
    parentFid: castAddBody.parentCastId?.fid,
    hash: msg.hash,
    parentHash: castAddBody.parentCastId?.hash,
    parentUrl: castAddBody.parentUrl,
    text: castAddBody.text,
    embeds: JSON.stringify(castAddBody.embeds),
    mentions: JSON.stringify(castAddBody.mentions),
    mentionsPositions: JSON.stringify(castAddBody.mentionsPositions),
  } satisfies Insertable<Tables['casts']>
}
