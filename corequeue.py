import uuid
import time
import redis


class Job(object):

    def __init__(self, msgid, msgdata, attempts, q):
        self.id = msgid
        self.data = msgdata
        self.attempts = attempts
        self.q = q
        self._result = None

    def complete(self):
        self.q.remove(self, completed=True)

    def error(self):
        if self.attempts < self.q.max_attempts:
            self.q.reschedule(self)
        else:
            self.q.remove(self, error=True)

    def defer(self):
        self.q.reschedule(self, keep_attempts=True)

    @property
    def result(self):
        if not self._result:
            self._result = eval(self.q.get_result(self))

        return self._result

    @result.setter
    def result(self, data):
        self.q.put_result(self, data)
        self._result = data

    def __repr__(self):
        s = {"id": self.id, "data": self.data, "attempts": self.attempts, "q": self.q.name}
        if self.q.result_suffix:
            s.extend({"result": self.result})

        return s

    def __str__(self):
        return str(self.__repr__())


class CoreQueue(object):

    def __init__(self, name=None, host="localhost", port=6379, with_results=False, with_ack=False, keep_dead=False, max_attempts=5, job_timeout=3600):
        self.max_attempts = max_attempts
        self.timeout = job_timeout

        self.backend = redis.StrictRedis(host=host, port=port)
        self.name = name or "default"
        #check to see if queue exists already
        if not self.backend.hexists("QUEUEREGISTER", self.name):
            self.backend.hset("QUEUEREGISTER", self.name, time.time())

        self.locked = self.name + ":LOCKED"
        self.attempts = self.name + ":ATTEMPTS"
        #optionally
        if with_results:
            self.result_suffix = ":RESULT"
        else:
            self.result_suffix = None
        if keep_dead:
            self.dead_msg_queue = self.name + ":DEAD"
        else:
            self.dead_msg_queue = None
        if with_ack:
            self.ack_suffix = ":ACK"
        else:
            self.ack_suffix = None

    def put(self, data):
        objkey = self.name + ":" + str(uuid.uuid4())

        r = self.backend.pipeline()
        r.set(objkey, data)
        r.lpush(self.name, objkey)
        r.execute()

    def next(self):
        self.clean_jobs()
        objkey = self.backend.rpop(self.name)

        if self.backend.hexists(self.locked, objkey):
            self.backend.rpush(self.name, objkey)
            raise ValueError("Problem with locking")
        else:
            self.backend.hset(self.locked, objkey, time.time())

        if self.backend.hexists(self.attempts, objkey):
            self.backend.hincrby(self.attempts, objkey, 1)
        else:
            self.backend.hset(self.attempts, objkey, 0)

        return Job(objkey, self.backend.get(objkey), self.backend.hget(self.attempts, objkey), self)

    def remove(self, job, error=False, completed=False):
        r = self.backend.pipeline()

        if error and self.dead_msg_queue:
            r.hset(self.dead_msg_queue, job.id, job.data)
        if completed and self.ack_suffix:
            r.hset(job.id + self.ack_suffix, "when", time.time())
            r.expire(job.id + self.ack_suffix, 3600)

        r.hdel(self.locked, job.id)
        r.hdel(self.attempts, job.id)
        r.delete(job.id)
        r.execute()

    def reschedule(self, job, keep_attempts=False):
        r = self.backend.pipeline()

        if not keep_attempts:
            r.hincrby(self.attempts, job.id, 1)

        r.hdel(self.locked, job.id)
        r.lpush(self.name, job.id)
        r.execute()

    def put_result(self, job, data):
        if not self.result_suffix:
            raise NotImplementedError("Cannot store results in this queue.")

        if not data:
            raise ValueError("Cannot save empty result.")

        result = bool(self.backend.hsetnx(job.id + self.result_suffix, "data", data))
        if not result:
            raise ValueError("Result exists already. Cannot be overwritten.")
        self.backend.expire(job.id + self.result_suffix, 3600*24)

    def get_result(self, job):
        if not self.result_suffix:
            raise NotImplementedError("No stored results in this queue.")

        return self.backend.get(job.id + self.result_suffix)

    def reset(self):
        items_to_delete = []
        for item in self.backend.lrange(self.name, 0, -1):
            items_to_delete.append(item)

        r = self.backend.pipeline()
        r.delete(self.attempts)
        r.delete(self.locked)
        r.delete(self.name)
        r.delete(items_to_delete)
        r.execute()

    def delete(self):
        self.reset()
        self.backend.hdel("QUEUEREGISTER", self.name)

    def clean_jobs(self):
        locks = self.backend.hgetall(self.locked)

        for item in locks:
            if eval(locks[item]) + self.timeout < time.time():
                self.backend.hdel(self.locked, item)
                if self.backend.exists(item):
                    self.backend.lpush(self.name, item)
