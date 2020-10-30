import { v4 } from "uuid"

export interface TaskInfo {
  uuid: string
}

export type TaskCb<Payload> = (i: Payload, info: TaskInfo) => Promise<any>

export interface Task<Payload> {
  logtxt: string
  data: Payload
  fn: (data: Payload, taskInfo: TaskInfo) => Promise<void>
  cb: (err, res) => void
  uuid: string
}

export class Queue<Payload> {
  onTimeout(data: Payload, taskInfo: TaskInfo) {}

  QUEUE_MAX_RUNNING = Number(process.env.QUEUE_MAX_RUNNING) || 5
  QUEUE_MAX_WAITING = Number(process.env.QUEUE_MAX_WAITING) || 10
  QUEUE_TIMEOUT = Number(process.env.QUEUE_TIMEOUT) || 60000

  waiting = [] as Task<Payload>[]
  running = [] as Task<Payload>[]

  logStatus(action: string, someid: string) {
    console.log(action, someid, `${this.running.length} running`, `${this.running.length} in queue`)
  }

  push<T>(logtxt: string, data: Payload, fn: TaskCb<Payload>) {
    if (this.waiting.length > this.QUEUE_MAX_WAITING) {
      throw Error("Fila cheia. Tente mais tarde.")
    }
    return new Promise<T>((res, rej) => {
      const cb = (err, out) => {
        if (err) return rej(err)
        res(out)
      }
      this.waiting.push({ logtxt, data, fn, cb, uuid: v4() })
      this.walk()
      if (logtxt) {
        this.logStatus("enqueueing", logtxt)
      }
    })
  }

  walk() {
    this.logStatus("walk next", "")
    if (this.running.length > this.QUEUE_MAX_RUNNING) return
    let next = this.waiting.shift()
    if (next) {
      this.running.push(next)
      const timeouted = new Promise((res, rej) => {
        setTimeout(() => {
          rej(new TimeoutError("timeout"))
        }, this.QUEUE_TIMEOUT)
      })
      const taskinfo = { uuid: next.uuid }
      Promise.race([next.fn(next.data, taskinfo), timeouted])
        .then((resp) => {
          this.logStatus("finished", next?.logtxt!)
          next?.cb(null, resp)
        })
        .catch((err) => {
          let text = "failed"
          if (err instanceof TimeoutError) {
            text += " by timeout"
            this.onTimeout(next?.data!, taskinfo)
          }
          this.logStatus(text, next?.logtxt!)
          next?.cb(err, null)
        })
        .finally(() => {
          const found = this.running.indexOf(next!)
          //might have been removed by other events
          if (found !== -1) {
            this.running.splice(found, 1)
            this.walk()
          }
        })
    }
  }
}

class TimeoutError extends Error {}
