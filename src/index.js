// @flow

import go from 'go-for-it'

type Json = number | boolean | string | null | { [key: string]: Json }

type JobId = string

type Job = {|
  id: JobId,
  data: Json,
  status: 'created' | 'done' | 'failed',
|}

type QueueId = string
type WorkerId = string

// Rejections must be done with some error
interface Handlers1 {
  // createJob must add a unique `id` attribute to jobs and store them.
  createJob: (QueueId, Json) => Promise<void>,
}
interface Handlers2 {
  // candidate must be added before the returned promise fulfills.
  addCandidate: (QueueId, WorkerId) => Promise<void>,
  // candidate must be removed before the ruterned promise fulfills.
  removeCandidate: (QueueId, WorkerId) => Promise<void>,
  getCandidateCount: JobId => Promise<number>,
  getJobs: QueueId => Promise<Array<Job>>,
  completeJob: JobId => Promise<void>,
  failJob: JobId => Promise<void>,
}
type Handlers = Handlers1 & Handlers2

type AddJob = (Handlers1, QueueId, Json) => Promise<void>
const addJob: AddJob = ({ createJob }, queueId, jobData) => createJob(queueId, jobData)

type Resolver = (?any) => void

type StoppablePromise<T> = {
  then: ((?Resolver, ?Resolver) => void) => Promise<T>,
  catch: ((?Resolver, ?Resolver) => void) => Promise<T>,
  stop: () => Promise<T>,
}

type Work = (Handlers2, QueueId, WorkerId, Job => Promise<void>) => StoppablePromise<void>
const work: Work = (
  { addCandidate, removeCandidate, getCandidateCount, getJobs, completeJob, failJob },
  queueId,
  workerId,
  processJob
) => {
  let active = true

  const working = new Promise((resolve, reject) => {
    const delay = s => new Promise((resolve) => { setTimeout(resolve, s) })

    const _work = async () => {
      const jobs = await getJobs(queueId)

      if (jobs.length === 0) return resolve()

      const job = jobs[0]
      await addCandidate(workerId, job.id)
      const count = await getCandidateCount(job.id)

      if (count >= 2) {
        await removeCandidate(workerId, job.id)
        await delay(100)
      } else {
        const [err] = await go(processJob(job))
        await (err ? failJob(job.id) : completeJob(job.id))
      }

      await removeCandidate(workerId, job.id)

      if (active) await _work(); else reject(Error('Worker has been stopped'))
    }

    _work().catch(reject)
  })

  return {
    then: working.then.bind(working),
    catch: working.catch.bind(working),
    stop: () => {
      active = false
      return working.catch(e => { if (e.message !== 'Worker has been stopped') throw e })
    }
  }
}

export { addJob, work }
export type { Job, Handlers }
