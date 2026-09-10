import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { nextScheduleOccurrence } from '~/lib/schedule.server'

describe('nextScheduleOccurrence', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('computes the next fire time in UTC by default', () => {
    // Daily at 02:00 — from 12:00 on the 15th, the next fire is 02:00 on the 16th.
    const next = nextScheduleOccurrence('0 2 * * *')
    expect(next?.toISOString()).toBe('2024-01-16T02:00:00.000Z')
  })

  it('honors the schedule timezone', () => {
    // 02:00 in New York (UTC-5 in January) is 07:00 UTC.
    const next = nextScheduleOccurrence('0 2 * * *', 'America/New_York')
    expect(next?.toISOString()).toBe('2024-01-16T07:00:00.000Z')
  })

  it('treats an empty timezone as UTC', () => {
    const next = nextScheduleOccurrence('0 2 * * *', '')
    expect(next?.toISOString()).toBe('2024-01-16T02:00:00.000Z')
  })

  it('returns a future date for a frequent schedule', () => {
    const next = nextScheduleOccurrence('*/15 * * * *')
    expect(next?.toISOString()).toBe('2024-01-15T12:15:00.000Z')
  })

  it('evaluates a recurrence rule, in its timezone', () => {
    // The rrule kind reads through the same method, so a rule row gets a real next occurrence
    // instead of the dash an expression the cron parser refuses would have produced.
    expect(nextScheduleOccurrence('FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0')?.toISOString())
      .toBe('2024-01-16T02:00:00.000Z')
    expect(nextScheduleOccurrence('FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0', 'America/New_York')?.toISOString())
      .toBe('2024-01-16T07:00:00.000Z')
  })

  it('returns null for a rule that has no occurrences left', () => {
    expect(nextScheduleOccurrence('FREQ=DAILY;UNTIL=20240101T000000Z')).toBeNull()
  })

  it('returns null for an unparseable expression', () => {
    expect(nextScheduleOccurrence('not a cron')).toBeNull()
    expect(nextScheduleOccurrence('99 99 99 99 99')).toBeNull()
    expect(nextScheduleOccurrence('FREQ=NONSENSE')).toBeNull()
  })
})
