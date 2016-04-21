#core imports
import uuid
import time
import random
try:
    from cPickle import cPickle as pickle
except:
    import pickle
#third-party imports
import redis
from redis.sentinel import Sentinel


class Job(object):
    """ Object used by the queue to return one message.
        Initialized by the queue.

        Attributes
        ----------
        id : string
            UUID4 string used by the queue to identify the message
        data : dict
            Dictionary containing the message data
        priority: string
            Name of the source queue for this message
    """

    def __init__(self, msgid, msgdata, priority, q):
        """ Init method for the Job class.

            Parameters
            ----------
            msgid : string
                UUID4 string used by the queue to identify the message
            msgdata: pickled string
                String with the pickled contents of the message
            priority: string
                Name of the source queue for this message
            q : object
                The queue object used
        """
        self.id = msgid
        self.data = pickle.loads(msgdata)
        self.priority = priority

        if not isinstance(q, CoreQueue):
            raise ValueError("Wrong queue class type")
        self._q = q
        self._result = None

    def complete(self):
        """ Method used to mark a message as completed.
            Depending on the queue settings this can generate an ack value.
        """

        self.q.remove(self.id, completed=True)

    def error(self):
        """Method used to mark an error in the user processing of the message.
           Depending on the max_attempts setting of the queue the message may be rescheduled.
           Depending on the keep_dead setting of the queue a message which has already reached its maximum number of attempts may be kept in the dead queue or not.
        """

        if self.attempts < self._q._max_attempts:
            self._q.reschedule(self.id, self.priority)
        else:
            self._q.remove(self.id, error=True)

    def defer(self):
        """Method used by the user to postpone any marking of the message and reschedule its processing.
        """

        self._q.reschedule(self, self.priority, keep_attempts=True)

    @property
    def result(self):
        """Property used to return the user submitted result of the message processing.
        """

        if not self._result:
            self._result = pickle.loads(self._q.get_result(self.id))

        return self._result

    @result.setter
    def result(self, data):
        """ Property setter used to set the user submitted result of the message processing.

            Parameters
            ----------
            data : any python object that can be pickled
                The data that the user wants to return after the message processing
        """

        self._q.put_result(self.id, pickle.dumps(data))
        self._result = data

    @property
    def attempts(self):
        """Property used to return the number of processing attempts for this message.
        """

        return self._q.get_attempts(self.id)

    @attempts.setter
    def attempts(self, data):
        raise NotImplementedError()

    def __repr__(self):
        s = {"id": self.id, "data": pickle.dumps(self.data), "attempts": self.attempts, "q": self._q.name}
        if self.q.result_suffix:
            s.update({"result": self.result})

        return s


class CoreQueue(object):
    """ A class used to abstract a FIFO queue on top of redis.
        If the REDIS infrastructure is distributed using Sentinel, it will use that option.

        Attributes
        ----------
        name : string
            The name of the queue

    """

    def __init__(
        self,
        name=None,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        socket_timeout=20,
        max_attempts=5,
        job_timeout=3600,
        distributed_name=None,
        with_results=False,
        with_ack=False,
        with_priority=False,
        keep_dead=False
    ):
        """ Init method for the CoreQueue class.

            Parameters
            ----------
            name : string
                The name of the queue
            host : string or list
                The address of the host or a list of addresses for sentinel instances
            port : int
                The active port for the redis or sentinel instance
            db : int
                The database identifier used by redis
            password : string
                The access password for redis
            socket_timeout : int
                The timeout observed when establishing a socket connection
            max_attempts : int
                The maximum number of attempts allowed for a queue item
            job_timeout : int
                The maximum number of seconds a queue waits before removing a lock for a unacknowledged job
            distributed_name : string
                The name of the sentinel service
            with_results : boolean
                Flag used to determine if a queue can also save and return the message processing results
            with_ack : boolean
                Flag used to determine if to create a queue that holds the completion acknowledgements of delivered messages
            with_priority : boolean
                Flag used to determine if to create a secondary queue that holds messages that need to be processed before those in the normal queue
            keep_dead : boolean
                Flag used to determine if the messages with errors or which have reached the maximum number of delivery attempts should be kept in a 'dead letter' queue
        """

        self._max_attempts = max_attempts
        self._job_timeout = job_timeout

        if distributed_name:
            self.sentinel = Sentinel(sentinels=host, socket_timeout=socket_timeout, password=password)
            self._backend = self.sentinel.master_for(distributed_name, socket_timeout=socket_timeout)
        else:
            self._backend = redis.StrictRedis(host=host, port=port, password=password, socket_timeout=socket_timeout)

        if not name or not type(name, str):
            raise ValueError("You need to supply a valid string as a name for the queue.")
        self.name = name
        #check to see if queue exists already
        if not self._backend.hexists("QUEUEREGISTER", self.name):
            self._backend.hset("QUEUEREGISTER", self.name, time.time())

        #required attributes
        self.locked = self.name + ":LOCKED"
        self.attempts = self.name + ":ATTEMPTS"

        #optional attributes
        if with_results:
            self.result_suffix = ":RESULT"
        else:
            self.result_suffix = None

        if keep_dead:
            self.dead = self.name + ":DEAD"
        else:
            self.dead = None

        if with_ack:
            self.ack = self.name + ":ACK"
        else:
            self.ack = None

        if with_priority:
            self.high = self.name + ":HIGH"
        else:
            self.high = None

    def put(self, data, high_priority=False):
        """ Method used to submit a message to the queue.
            Returns a Job object with all the info regarding the message.

            Parameters
            ----------
            data : object
                The body of the message, any python object that can be pickled
            high_priority : boolean
                Flag used to signal if the message should be processed with high priority
        """

        if not high_priority:
            objkey = self.name + ":" + str(uuid.uuid4())
            q = self.name
        else:
            objkey = self.high + ":" + str(uuid.uuid4())
            q = self.high

        r = self._backend.pipeline()
        r.set(objkey, pickle.dumps(data))
        r.lpush(q, objkey)
        r.execute()

        return Job(objkey, data, 0, self)

    def next(self, ignore_high=False):
        """ Method used to return the next job in the queue.
            If the queue has high priority set then items from the high priority queue are preferred.
            The user can choose to ignore the high priority setting and will get an item from a random queue.

            Parameters
            ----------
            ignore_high : boolean
                Flag used to signal that the next message should ignore the high priority setting
        """

        self.clean_jobs()

        if self.high and not ignore_high:
            if self._backend.llen(self.high) > 0:
                q = self.high
            else:
                q = self.name
        else:
            if self.high and self._backend.llen(self.high) > 0:
                q = random.choice((self.name, self.high))
            else:
                q = self.name

        objkey = self._backend.rpop(q)

        if self._backend.hexists(self.locked, objkey):
            self._backend.rpush(q, objkey)
            raise ValueError("Problem with locking")
        else:
            self._backend.hset(self.locked, objkey, time.time())

        self._backend.hincrby(self.attempts, objkey, 1)

        return Job(objkey, self._backend.get(objkey), q, self)

    def remove(self, jobid, error=False, completed=False):
        """ Method used to remove a message from the queue.
            If any of the flags is set to true then the result is processed accordingly.

            Parameters
            ----------
            jobid : string
                The unique id assigned by the queue to the message
            error : boolean
                Flag used to signal that the message cannot be completed and has reached the maximum numbers of attempts
            completed : boolean
                Flag used to signal that the message was completed
        """

        r = self._backend.pipeline()

        if error and completed:
            raise ValueError("Cannot mark a message with both error and complete.")
        if error and self.dead:
            r.hset(self.dead, jobid, time.time())
        if completed and self.ack:
            r.hset(self.ack, jobid, time.time())

        r.hdel(self.locked, jobid)
        r.hdel(self.attempts, jobid)
        r.delete(jobid)
        r.execute()

    def reschedule(self, jobid, q, keep_attempts=False):
        """ Method used to reschedule a message in the queue.
            If the keep_attempts flag is True, then the number of attempts for the message will not be increased.
            The queue were the message will be inserted depends on the source queue of the item.

            Parameters
            ----------
            jobid : string
                The unique id assigned by the queue to the message
            q : string
                Name of the queue where the job needs to be rescheduled
            keep_attempts : boolean
                Flag used to indicate whether the number of attempts for the job should be increased or not
        """

        r = self._backend.pipeline()

        if not keep_attempts:
            r.hincrby(self.attempts, jobid, 1)

        r.hdel(self.locked, jobid)
        r.lpush(q, jobid)
        r.execute()

    def put_result(self, jobid, data):
        """ Method used to save a result from a message processing.

            Parameters
            ----------
            jobid : string
                The unique id assigned by the queue to the message
            data : object
                Any python data object that can be pickled
        """

        if not self.result_suffix:
            raise NotImplementedError("Cannot store results in this queue.")

        if not data:
            raise ValueError("Cannot save empty result.")

        result = bool(self._backend.setnx(jobid + self.result_suffix, pickle.dumps(data)))
        if not result:
            raise ValueError("Result exists already. Cannot be overwritten.")
        self._backend.expire(jobid + self.result_suffix, 3600*24)

    def get_result(self, jobid):
        """ Method used to retrieve a result from a message processing.

            Parameters
            ----------
            jobid : string
                The unique id assigned by the queue to the message
        """

        if not self.result_suffix:
            raise NotImplementedError("No stored results in this queue.")

        return self._backend.get(jobid + self.result_suffix)

    def get_attempts(self, jobid):
        """ Method used to retrieve the current number of attempts for a given message id.

            Parameters
            ----------
            jobid : string
                The unique id assigned by the queue to the message
        """

        return self._backend.hget(self.attempts, jobid)

    def reset(self):
        """ Method used to reset the queue, deleting all the messages in the queue and resetting all the other queues and counters.
        """

        items_to_delete = []
        for item in self._backend.lrange(self.name, 0, -1):
            items_to_delete.append(item)

        r = self._backend.pipeline()
        r.delete(self.attempts)
        r.delete(self.locked)
        r.delete(self.name)
        if self.high:
            r.delete(self.high)
        r.delete(items_to_delete)
        r.execute()

    def delete(self):
        """ Method used to delete a queue.
        """

        self.reset()
        self._backend.hdel("QUEUEREGISTER", self.name)

    def clean_jobs(self):
        """Method used to check the locked job register and reschedule appropriate ones.
           Job_timeout is used to set an expiry interval.
        """

        locks = self._backend.hgetall(self.locked)

        for item in locks:
            if eval(locks[item]) + self._job_timeout < time.time():
                self._backend.hdel(self.locked, item)
                if self._backend.exists(item):
                    if self.high and self.high in item:
                        self._backend.lpush(self.high, item)
                    else:
                        self._backend.lpush(self.name, item)

    def size(self):
        """Method used to get the current size of the queue.
        """

        self.clean_jobs()
        if not self.high:
            return self._backend.llen(self.name)
        else:
            return self._backend.llen(self.name) + self._backend.llen(self.high)
