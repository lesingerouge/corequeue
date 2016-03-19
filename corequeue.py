import uuid
import time
import random
import redis


class Job(object):

    def __init__(self, msgid, msgdata, attempts, q):
        self.id = msgid
        self.data = msgdata
        self.attempts = attempts
        self.q = q
        self._result = None

    def complete(self):
        self.q.remove(self.id, completed=True)

    def error(self):
        if self.attempts < self.q.max_attempts:
            self.q.reschedule(self.id)
        else:
            self.q.remove(self.id, error=True)

    def defer(self):
        self.q.reschedule(self, keep_attempts=True)

    @property
    def result(self):
        if not self._result:
            self._result = json.loads(self.q.get_result(self.id))

        return self._result

    @result.setter
    def result(self, data):
        self.q.put_result(self.id, json.dumps(data))
        self._result = data

    def __repr__(self):
        s = {"id": self.id, "data": self.data, "attempts": self.attempts, "q": self.q.name}
        if self.q.result_suffix:
            s.extend({"result": self.result})

        return s

    def __str__(self):
        return str(self.__repr__())


class CoreQueue(object):

    def __init__(self, name=None, host="localhost", port=6379, with_results=False, with_ack=False, with_priority=False, keep_dead=False, max_attempts=5, job_timeout=3600):
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
        self.result_suffix = None if not with_results else ":RESULT"
        self.dead_msg_queue = None if not keep_dead else self.name + ":DEAD"
        self.ack_suffix = None if not with_ack else ":ACK"
        self.high = None if not with_priority else self.name + ":HIGH"

    def put(self, data, high_priority=False):
        q = self.name if not high_priority else self.high
        objkey = q + ":" + str(uuid.uuid4()) 

        r = self.backend.pipeline()
        r.set(objkey, data)
        r.lpush(q, objkey)
        r.execute()

        return Job(objkey, data, 0, self)

    def next(self, ignore_high=False):
        self.clean_jobs()
        if not ignore_high:
            q = self.name if self.backend.llen(self.high) == 0 else self.high
        else:
            q = random.choice((self.name, self.high))

        objkey = self.backend.rpop(q)

        if self.backend.hexists(self.locked, objkey):
            self.backend.rpush(q, objkey)
            raise ValueError("Problem with locking")
        else:
            self.backend.hset(self.locked, objkey, time.time())

        self.backend.hincrby(self.attempts, objkey, 1)
        
        return Job(objkey, self.backend.get(objkey), self.backend.hget(self.attempts, objkey), self)

    def remove(self, jobid, error=False, completed=False):
        r = self.backend.pipeline()

        if error and self.dead_msg_queue:
            r.hset(self.dead_msg_queue, jobid, job.data)
        if completed and self.ack_suffix:
            r.hset(jobid + self.ack_suffix, "when", time.time())
            r.expire(jobid + self.ack_suffix, 3600)

        r.hdel(self.locked, jobid)
        r.hdel(self.attempts, jobid)
        r.delete(jobid)
        r.execute()

    def reschedule(self, jobid, keep_attempts=False):
        r = self.backend.pipeline()

        if not keep_attempts:
            r.hincrby(self.attempts, jobid, 1)

        r.hdel(self.locked, jobid)
        if self.high in jobid:
            r.lpush(self.high, jobid)
        else:
            r.lpush(self.name, jobid)
        r.execute()

    def put_result(self, jobid, data):
        if not self.result_suffix:
            raise NotImplementedError("Cannot store results in this queue.")

        if not data:
            raise ValueError("Cannot save empty result.")

        result = bool(self.backend.hsetnx(jobid + self.result_suffix, "data", data))
        if not result:
            raise ValueError("Result exists already. Cannot be overwritten.")
        self.backend.expire(jobid + self.result_suffix, 3600*24)

    def get_result(self, jobid):
        if not self.result_suffix:
            raise NotImplementedError("No stored results in this queue.")

        return self.backend.get(jobid + self.result_suffix)

    def reset(self):
        items_to_delete = self.backend.lrange(self.name, 0, -1)
        if self.high:
            items_to_delete += self.backend.lrange(self.high, 0, -1)
            
        r = self.backend.pipeline()
        r.delete(self.attempts)
        r.delete(self.locked)
        r.delete(self.name)
        if self.high:
            r.delete(self.high)
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
                    if self.high in item:
                        self.backend.lpush(self.high, item)
                    else:
                        self.backend.lpush(self.name, item)

    def size(self):
        self.clean_jobs()
        if not self.high:
            return self.backend.llen(self.name)
        else:
            return self.backend.llen(self.name) + self.backend.llen(self.high)
