"""
    An RPC service-oriented architecture model with SimPy

    Based in large part on packet-switching queueing work
    by Greg M. Bernstein

    https://www.grotto-networking.com/DiscreteEventPython.html
    Copyright 2014 Greg M. Bernstein
    Released under the MIT license
"""
import simpy
import random
import functools
from heapq import heappush, heappop

class Request(object):
    """ A very simple class that represents an inbound RPC request
        This request will run through a some load-balancers and
        services and eventually be "completed."

        Requests have a "cost" which is the final cost of "completion."
        We use a float to represent the size of the packet in bytes so that
        we can compare to ideal M/M/1 queues.

        Parameters
        ----------
        time : float
            the time the packet arrives at the output queue.
        size : float
            the size of the packet in bytes
        id : int
            an identifier for the packet
        completed : float
            the time at which the request is marked "completed."
    """
    def __init__(self, time, size, id,  src="a"):
        self.time = time
        self.size = size
        self.id = id
        self.src = src
        self.completed = None #timestamp
        self.completion_cbs = [] # callback(s) for action at completion
        self.completion_event = None

    def __repr__(self):
        return "id: {}, size: {}, time: {}, src: {} completed: {}".\
            format(self.id, self.size, self.time, self.src, self.completed)

    def complete(self, timestamp):
        self.completed = timestamp;

class RequestGenerator(object):
    """ Generates requests with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the request.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        adist : function
            a no parameter function that returns the successive inter-arrival times of the request
        sdist : function
            a no parameter function that returns the successive sizes of the request
        initial_delay : number
            Starts generation after an initial delay. Default = 0
        finish : number
            Stops generation at the finish time. Default is infinite

        Note: requests are assigned IDs based on the order that they're generated.
    """
    def __init__(self, env, id,  adist, sdist, initial_delay=0, finish=float("inf")):
        self.id = id
        self.env = env
        self.adist = adist
        self.sdist = sdist
        self.initial_delay = initial_delay
        self.finish = finish
        self.out = None
        self.requests_sent = 0
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        """The generator function used in simulations.
        """
        yield self.env.timeout(self.initial_delay)
        while self.env.now < self.finish:
            # wait for next transmission
            yield self.env.timeout(self.adist())
            self.requests_sent += 1
            req = Request(self.env.now, self.sdist(), self.requests_sent, src=self.id)
            req.completion_event = self.env.event()
            self.out.put(req)

class RequestSink(object):
    """ Receives requests and collects delay information into the
        waits list. You can then use this list to look at delay statistics.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each request will be printed as it is received.
        rec_arrivals : boolean
            if true then arrivals will be recorded
        absolute_arrivals : boolean
            if true absolute arrival times will be recorded, otherwise the time between consecutive arrivals
            is recorded.
        rec_waits : boolean
            if true waiting time experienced by each requests is recorded
        selector: a function that takes a request and returns a boolean
            used for selective statistics. Default none.

    """
    def __init__(self, env, rec_arrivals=False, absolute_arrivals=False, rec_waits=True, debug=False, selector=None):
        self.store = simpy.Store(env)
        self.env = env
        self.rec_waits = rec_waits
        self.rec_arrivals = rec_arrivals
        self.absolute_arrivals = absolute_arrivals
        self.waits = []
        self.arrivals = []
        self.debug = debug
        self.requests_rec = 0
        self.bytes_rec = 0
        self.selector = selector
        self.last_arrival = 0.0

    def put(self, pkt):
        if not self.selector or self.selector(pkt):
            now = self.env.now
            if self.rec_waits:
                self.waits.append(self.env.now - pkt.time)
            if self.rec_arrivals:
                if self.absolute_arrivals:
                    self.arrivals.append(now)
                else:
                    self.arrivals.append(now - self.last_arrival)
                self.last_arrival = now
            self.requests_rec += 1
            self.bytes_rec += pkt.size
            if self.debug:
                print(pkt)

class SynchronousForwarder(object):
    """ Forwards requests one at a time, in order of receipt. each
        forwarded request waits on completion.

        Set the "out" member variable to the entity to receive the request.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
    """
    def __init__(self, env, N=1, debug=False):
        self.env = env
        self.out = None
        self.requests_rec = 0
        self.slots = simpy.Resource(env, capacity=N)
        self.debug = debug

    def putAndWait(self, req, event):
        slot = self.slots.request()
        yield slot
        self.out.put(req)
        yield event
        self.slots.release(slot)
        return req

    def put(self, req):
        print("trying forwarding put")
        self.requests_rec += 1
        self.env.process(self.putAndWait(req, req.completion_event))
        print("forwarded!")
        return True

class RequestCompleter(object):
    """ Completes requests one at a time, in order of receipt.
        Set the "out" member variable to the entity to receive the request.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
    """
    def __init__(self, env, debug=False):
        self.store = simpy.Store(env)
        self.env = env
        self.out = None
        self.requests_rec = 0
        self.time_spent = 0.0
        self.debug = debug
        self.hiwat = 0
        self.extra_delay_factor = 1.0
        self.busy = 0  # Used to track if a request is currently being sent
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            req = (yield self.store.get())
            self.busy = 1
            self.time_spent += req.size
            yield self.env.timeout(req.size * self.extra_delay_factor)
            for c in req.completion_cbs:
                c()
            req.completed = self.env.now
            self.out.put(req)
            self.busy = 0
            if self.debug:
                print(req)

    def inflight(self):
        return len(self.store.items) + busy

    def put(self, req):
        self.requests_rec += 1
        if self.hiwat < len(self.store.items):
            self.hiwat = len(self.store.items)
        return self.store.put(req)

class RandomBrancher(object):
    """ A demultiplexing element that chooses the output at random.

        Contains a list of output ports of the same length as the probability list
        in the constructor.  Use these to connect to other network elements.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        outputs : List
            list of outputs
    """
    def __init__(self, env, outputs):
        self.env = env

        self.outs = outputs
        self.n_outs = len(outputs)
        self.out_stats = [0]*self.n_outs
        self.requests_rec = 0

    def put(self, req):
        self.requests_rec += 1
        rand = random.randint(0, self.n_outs - 1)
        self.outs[rand].put(req)
        self.out_stats[rand] += 1

    def __repr__(self):
        return "output stats by port " + str(out_stats)

class PQueueBrancher(object):
    """ A demultiplexing element that chooses the output-channel based on the
        output-channel's count of inflight requests.

        Contains a list of output ports of the same length as the probability list
        in the constructor.  Use these to connect to other network elements.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        outputs : List
            list of outputs
    """
    def __init__(self, env, outputs):
        self.env = env

        self.outs = outputs
        self.n_outs = len(outputs)
        self.out_stats = [0]*self.n_outs
        self.inflight = [0]*self.n_outs
        self.requests_rec = 0

    def decrement(self, i):
        self.inflight[i] -= 1

    def put(self, req):
        self.requests_rec += 1

        # build our heap, priority-keyed by inflight-count
        h = []
        for i in range(self.n_outs):
            heappush(h, (self.inflight[i], i))

        priority, index = heappop(h);
        # Allow the completer to tell us it finished.
        self.inflight[index] += 1
        req.completion_cbs.append(lambda b=self, i=index: b.decrement(i))
        self.outs[index].put(req)
        self.out_stats[index] += 1

        def __repr__(self):
            return "output stats by port " + str(out_stats)
