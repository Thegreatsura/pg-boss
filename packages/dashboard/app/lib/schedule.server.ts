import { PgBoss } from 'pg-boss'

// pg-boss evaluates both schedule kinds — cron expressions and RRULE recurrence rules — and
// previewSchedule() is the method it exposes for asking when one fires next. It is pure computation:
// the walk never touches the database, and passing `from` keeps it off the one property (the
// database clock) that would need a live connection. So the dashboard borrows the core's own
// evaluator through an instance that is constructed and never started, rather than reimplementing
// either parser and drifting from what the schedule pass will actually do.
const evaluator = new PgBoss({ connectionString: 'postgres://pg-boss-dashboard/unused' })

/**
 * Next time a schedule fires, evaluated in its own timezone, or null when there is no next time:
 * an expression neither parser accepts, or a finite rule with nothing left to send. Both cases are
 * a dash in the UI rather than a failed page.
 *
 * Defaults to UTC when the row carries no timezone, which is what the core does with a schedule
 * written before the column was validated.
 */
export function nextScheduleOccurrence (expression: string, timezone?: string | null): Date | null {
  try {
    const [next] = evaluator.previewSchedule(expression, {
      tz: timezone || 'UTC',
      count: 1,
      from: new Date()
    })
    return next ?? null
  } catch {
    return null
  }
}
