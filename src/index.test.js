// @flow

import assert from 'assert'
import { values } from 'ramda'
import { addJob, work, type Handlers, type Job } from './index.js'

const testingEnv = () => {
  const jobs: { [string]: Array<Job> } = {} // QueueId: [Job]
  const candidates: { [string]: Array<string> } = {} // JobId: [WorkerId]
  let jobId = '0'

  const next = () => {
    jobId = `${parseInt(jobId, 10) + 1}`
    return jobId
  }

  const handlers: Handlers = {
    createJob: (q, d) => {
      jobs[q] = jobs[q] ? jobs[q] : []
      const job = { id: next(), data: d, status: 'created' }
      jobs[q].push(job)
      return Promise.resolve()
    },
    addCandidate: (workerId, jobId) => {
      candidates[jobId] = candidates[jobId]
        ? candidates[jobId].concat(workerId)
        : [workerId]

      return Promise.resolve()
    },
    removeCandidate: (workerId, jobId) => {
      candidates[jobId] = candidates[jobId]
        ? candidates[jobId].filter(id => id !== workerId)
        : []

      return Promise.resolve()
    },
    getCandidateCount: jobId => Promise.resolve(candidates[jobId]
      ? candidates[jobId].length
      : 0),
    getJobs: queueId => Promise.resolve(jobs[queueId]
      ? jobs[queueId].filter(j => !['done', 'failed'].includes(j.status))
      : []),
    completeJob: (jobId) => {
      const flatten = xss => xss.reduce((flat, xs) => flat.concat(xs), [])
      const flatJobs: Array<Job> = flatten(values(jobs))
      flatJobs
        .filter(j => j.id === jobId)
        .forEach(j => { j.status = 'done' })

      return Promise.resolve()
    },
    failJob: (jobId) => {
      const flatten = xss => xss.reduce((flat, xs) => flat.concat(xs), [])
      const flatJobs: Array<Job> = flatten(values(jobs))
      flatJobs
        .filter(j => j.id === jobId)
        .forEach(j => { j.status = 'failed' })

      return Promise.resolve()
    }
  }

  const curriedAddJob = (queue, job) => addJob(
    handlers,
    queue,
    job
  )

  const curriedWork = (queue, worker, handler) => work(
    handlers,
    queue,
    worker,
    handler
  )

  return { jobs, candidates, work: curriedWork, addJob: curriedAddJob }
}

describe('Skew', () => {
  describe('.addJob', () => {
    it('adds job as specified by supplied `addJob` function', () => {
      const { jobs, addJob } = testingEnv()
      assert.equal(jobs['queueId'], undefined)
      addJob('queueId', '')
      assert.ok(jobs['queueId'])
      assert.equal(jobs['queueId'].length, 1)
    })
  })

  describe('.work', () => {
    it('updates the job to done if worker succeeds', done => {
      const { addJob, work, jobs } = testingEnv()
      addJob('queueId', '')
      work('queueId', 'workerId', () => Promise.resolve())
        .then(() => {
          assert.ok(jobs['queueId'])
          assert.equal(jobs['queueId'].length, 1)
          assert.equal(jobs['queueId'][0].status, 'done')
          done()
        })
    })

    it('updates the job to failed if worker fails', done => {
      const { addJob, work, jobs } = testingEnv()
      addJob('queueId', '')
      work('queueId', 'workerId', () => Promise.reject(Error()))
        .then(() => {
          assert.ok(jobs['queueId'])
          assert.equal(jobs['queueId'].length, 1)
          assert.equal(jobs['queueId'][0].status, 'failed')
          done()
        })
    })

    it('keeps processing jobs', done => {
      const { addJob, work, jobs } = testingEnv()
      addJob('queueId', '')
      addJob('queueId', '')
      addJob('queueId', '')
      work('queueId', 'workerId', () => Promise.resolve())
        .then(() => {
          assert.ok(jobs['queueId'])
          assert.equal(jobs['queueId'].length, 3)
          assert.equal(jobs['queueId'][0].status, 'done')
          assert.equal(jobs['queueId'][1].status, 'done')
          assert.equal(jobs['queueId'][2].status, 'done')
          done()
        })
    })

    describe('#stop', () => {
      it('gracefully stops processing jobs', done => {
        const { addJob, work, jobs } = testingEnv()
        addJob('queueId', '')
        addJob('queueId', '')
        addJob('queueId', '')
        work('queueId', 'workerId', () => Promise.resolve())
          .stop()
          .then(() => {
            assert.ok(jobs['queueId'])
            assert.equal(jobs['queueId'].length, 3)
            assert.equal(jobs['queueId'][0].status, 'done')
            assert.equal(jobs['queueId'][1].status, 'created')
            assert.equal(jobs['queueId'][2].status, 'created')
            done()
          })
      })
    })
  })
})
