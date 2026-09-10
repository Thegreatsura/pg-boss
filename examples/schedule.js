import { PgBoss } from '../src/index.ts'
import * as helper from '../test/testHelper.ts'

async function schedule () {
  const boss = new PgBoss(helper.getConnectionString())

  boss.on('error', console.error)

  await boss.start()

  const queue = 'scheduled-queue'

  await boss.createQueue(queue)

  await boss.schedule(queue, '*/2 * * * *', { arg1: 'schedule me' })

  // The same call takes an RFC 5545 recurrence rule, for the schedules cron cannot express. Every
  // two minutes on weekdays, in this case.
  await boss.schedule(queue, 'FREQ=MINUTELY;INTERVAL=2;BYDAY=MO,TU,WE,TH,FR', { arg1: 'rrule' }, { key: 'rrule' })

  await boss.work(queue, async ([job]) => {
    console.log(`received job ${job.id} with data ${JSON.stringify(job.data)} on ${new Date().toISOString()}`)
  })
}

schedule()
  .catch(err => {
    console.log(err)
    process.exit(1)
  })
