import random
import functools
import simpy

from SimComponents import RequestGenerator, RequestSink, RequestCompleter, SynchronousForwarder, RandomBrancher

if __name__ == '__main__':
    # random-variables for distribution of requests
    # inter-request-time distribution
    idist = functools.partial(random.expovariate, 2.0)
    # request cost distribution
    mean_cost = 100
    cdist = functools.partial(random.expovariate, 1.0/mean_cost)

    # Create the SimPy environment. This is the thing that runs the simulation.
    env = simpy.Environment()

    gen = RequestGenerator(env, "external", idist, cdist, initial_delay=50)

    # Create the packet generators and sink
    def selector(req):
        return req.src == "external"

    sink = RequestSink(env, debug=True, rec_arrivals=True, selector=selector)

    completer = RequestCompleter(env)
    completer.out = sink

    forwarder = SynchronousForwarder(env, N=1)
    forwarder.out = completer

    gen.out = forwarder
    env.run(until=4000)
