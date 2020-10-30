export class Queue<Data> {
  onTimeout(data: Data) {}

  QUEUE_MAX_RUNNING = Number(process.env.QUEUE_MAX_RUNNING) || 5
  QUEUE_MAX_WAITING = Number(process.env.QUEUE_MAX_WAITING) || 10
  QUEUE_TIMEOUT = Number(process.env.QUEUE_TIMEOUT)

  waiting = [] as { data: Data; fn: (data: Data) => Promise<void>; cb: (err, res) => void }[]
  running = [] as { data: Data; fn: (data: Data) => Promise<void>; cb: (err, res) => void }[]

  push(logtxt: string, data: Data, fn: (i: Data) => Promise<void>) {
    if (this.waiting.length > this.QUEUE_MAX_WAITING) {
      throw Error("Fila cheia. Tente mais tarde.")
    }
    return new Promise((res, rej) => {
      const cb = (err, out) => {
        if (err) return rej(err)
        res(out)
      }
      this.waiting.push({ data, fn, cb })
      this.walk()
      if (logtxt) {
        console.log(
          "enqueueing",
          logtxt,
          `${this.running.length} running`,
          `${this.running.length} in queue`
        )
      }
    })
  }

  walk() {
    if (this.running.length > this.QUEUE_MAX_RUNNING) return
    let next = this.waiting.shift()
    if (next) {
      this.running.push(next)
      const timeouted = new Promise((res, rej) => {
        setTimeout(() => {
          console.log("timeout", next?.data)
          rej(Error("timeout"))
        }, this.QUEUE_TIMEOUT)
      })
      Promise.race([next.fn(next.data), timeouted])
        .then((resp) => next?.cb(null, resp))
        .catch((err) => next?.cb(err, null))
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
