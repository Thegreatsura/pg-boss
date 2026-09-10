import { CronExpressionParser } from 'cron-parser'

/**
 * Asserts that `tz` is a time zone scheduling can actually use.
 *
 * cron-parser validates `tz` lazily: parsing without a reference date never constructs a CronDate,
 * so every string is accepted and a bad zone only surfaces later, when a date is computed, as an
 * opaque "CronDate: unhandled timestamp". Passing a reference date here forces that construction so
 * a typo like 'America/New_Yrok' is rejected by schedule() rather than persisted to the schedule
 * table. Deliberately reuses cron-parser rather than an independent Intl check, so what schedule()
 * accepts is exactly what the cron pass can evaluate.
 *
 * A recurrence rule is evaluated by rrule-temporal rather than cron-parser and is judged here all
 * the same: both resolve IANA zone names, and rrule-temporal ignores the zone it is handed whenever
 * DTSTART names one of its own, so nothing else judges the value the schedule stores.
 *
 * Callers validate the expression first, so a failure here is attributable to the zone.
 */
export function assertTimezone (tz: string): void {
  // Quoted so an empty string renders as `""` rather than a dangling colon
  const unusable = `Unknown or unsupported time zone: ${typeof tz === 'string' ? `"${tz}"` : String(tz)}`

  // A non-string zone earns no failure from the parse below: cron-parser reads it as "unset" and
  // quietly evaluates in the host's local zone. The schedule.timezone column is nullable, so a row
  // written before schedule() validated zones reads back as null, and evaluating or previewing it
  // would be right only on a host that happens to run in the zone that was meant.
  if (typeof tz !== 'string') {
    throw new Error(unusable)
  }

  try {
    CronExpressionParser.parse('* * * * *', { tz, strict: false, currentDate: new Date() })
  } catch {
    throw new Error(unusable)
  }
}
