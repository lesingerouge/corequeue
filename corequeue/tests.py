import redis
from corequeue import CoreQueue


def test_queue_init():

    testq = CoreQueue()

    conn = redis.StrictRedis(host="localhost", port=6379)

    assert conn.exists("QUEUEREGISTER") is True
    assert conn.hexists("QUEUEREGISTER", "default") is True

    conn.flushall()


def test_queue_put():

    testq = CoreQueue()
    testq.put({"test": "test"})

    conn = redis.StrictRedis(host="localhost", port=6379)

    assert conn.llen(testq.name) == 1
    assert len(conn.keys(testq.name)) == 1

    conn.flushall()


def test_queue_next():

    testq = CoreQueue()
    testq.put({"test": "test"})
    testq.put({"test2": "test2"})

    t = testq.next()
    assert eval(t.data) == {"test": "test"}
    g = testq.next()
    assert eval(g.data) == {"test2": "test2"}

    conn = redis.StrictRedis(host="localhost", port=6379)

    assert conn.hlen(testq.locked) == 2
    assert conn.llen(testq.name) == 0

    conn.flushall()


def test_queue_jobcomplete():

    conn = redis.StrictRedis(host="localhost", port=6379)

    testq = CoreQueue()
    testq.put({"test": "test"})
    testq.put({"test2": "test2"})

    t = testq.next()
    t.complete()

    assert conn.llen(testq.name) == 1
    assert len(conn.keys(testq.name+":*")) == 1

    g = testq.next()
    g.complete()

    assert conn.llen(testq.name) == 0
    assert len(conn.keys(testq.name+":*")) == 0

    conn.flushall()


def test_queue_jobdefer():

    conn = redis.StrictRedis(host="localhost", port=6379)

    testq = CoreQueue()
    testq.put({"test": "test"})

    t = testq.next()
    t.defer()

    assert conn.llen(testq.name) == 1
    assert len(conn.keys(testq.name)) == 1
    assert int(conn.hget(testq.attempts, t.id)) == 0

    conn.flushall()


def test_queue_joberror():

    conn = redis.StrictRedis(host="localhost", port=6379)

    testq = CoreQueue(keep_dead=True)
    testq.put({"test": "test"})

    t = testq.next()
    t.error()

    assert conn.llen(testq.name) == 0
    assert len(conn.keys(testq.name+":*")) == 1
    assert conn.hlen(testq.dead_msg_queue) == 1

    conn.flushall()


if __name__ == "__main__":
    test_queue_init()
    test_queue_put()
    test_queue_next()
    test_queue_jobcomplete()
    test_queue_jobdefer()
    test_queue_joberror()
    print "Passing all tests!"
