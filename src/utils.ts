import { fromFarcasterTime } from '@farcaster/hub-nodejs'

export function farcasterTimeToDate(time: number): Date {
  const result = fromFarcasterTime(time)
  if (result.isErr()) throw result.error
  return new Date(result.value)
}
