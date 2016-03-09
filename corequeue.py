import json
import uuid
import time
import redis


class CoreQueue(object):

    def __init__(self, name=None, host="localhost", port=6379, with_results=False, max_attempts=5, job_timeout=3600, keep_dead=False):
        self.max_attempts = max_attempts
        self.timeout = job_timeout
        self.name = name or "default"
        self.locked = self.name + ":LOCKED"
        self.attempts = self.name + ":ATTEMPTS"
        #auxiliary queues
        if with_results:
            self.results_queue = self.name + "_res"
        else:
            self.results_queue = None
        if keep_dead:
            self.dead_msg_queue = self.name + "dead_msg"
        else:
            self.dead_msg_queue = None

        self.backend = redis.StrictRedis(host=host, port=port)

    def put(self, data):
        objkey = self.name + ":" + uuid.uuid4()
        temp = json.dumps(data)

        r = self.backend.pipeline()
        r.set(objkey, temp)
        r.lpush(self.name, objkey)
        r.execute()

    def next(self):
        self.clean_jobs()
        objkey = self.backend.rpop(self.name)

        if self.backend.hexists(self.locked, objkey):
            raise ValueError("Problem with locking")
        else:
            self.backend.hset(self.locked, objkey, time.time())

        if self.backend.hexists(self.attempts, objkey):
            self.backend.hincrby(self.attempts, objkey, 1)
        else:
            self.backend.hset(self.attempts, objkey, 0)

        return Job(objkey, self.backend.get(objkey), self.backend.hget(self.attempts, objkey), self)

    def remove(self, job, error=False):
        if error and self.dead_msg_queue:
            self.backend.lset(self.dead_msg_queue, job.data)

        r = self.backend.pipeline()
        r.hdel(self.locked, job.id)
        r.hdel(self.attempts, job.id)
        r.hdel(self.results_queue, job.id)
        r.delete(job.id)
        r.execute()

    def clean_jobs(self):
        locks = self.backend.hgetall(self.locked)

        for item in locks:
            if locks[item] + self.timeout > time.time():
                self.backend.hdel(self.locked, item)
                if self.backed.exists(item):
                    self.backend.lpush(item, self.backend.get(item))


class Job(object):

    def __init__(self, msgid, msgdata, attempts, queue):
        self.id = msgid
        self.data = msgdata
        self.attempts = attempts
        self.queue = queue
        self._result = None

    def complete(self):
        self.queue.remove(self)

    def error(self, message=None):
        if self.attempts < self.queue._max_attempts:
            self.queue.reschedule(self, message=message)
        else:
            self.queue.remove(self)

    def defer(self):
        self.queue.reschedule(self, no_attempt=True)

    @property
    def result(self):
        if not self._result:
            self._result = self.queue._backend.get_result(self.id)

        return self._result

    @result.setter
    def result(self, data):
        resultsq = self.queue._results_queue
        if not resultsq:
            raise NotImplementedError("Cannot store results for this job.")

        self.queue._backend.put_result(data)
        self._result = data
