import random
import functools
import simpy

from SimComponents import RequestGenerator, RequestSink, RequestCompleter, RandomBrancher

if __name__ == '__main__':
    # random-variables for distribution of requests
    # inter-request-time distribution
    idist = functools.partial(random.expovariate, 2.0)
    # request cost distribution
    mean_cost = 10
    cdist = functools.partial(random.expovariate, 1.0/mean_cost)

    # Create the SimPy environment. This is the thing that runs the simulation.
    env = simpy.Environment()

    gen = RequestGenerator(env, "external", idist, cdist, initial_delay=50, finish=5000)

    # Create the packet generators and sink
    def selector(req):
        return req.src == "external"
    sink = RequestSink(env, debug=True, rec_arrivals=True, selector=selector)

    n_completers = 15
    completers = [None]*n_completers
    for i in range(n_completers):
        completers[i] = RequestCompleter(env)
        completers[i].out = sink

    completers[0].extra_delay_factor = 2.5
    brancher = PQueueBrancher(env, completers)
    gen.out = brancher
    env.run(until=25000)

    for i in range(n_completers):
        print ("{} reqs: {} hiwater {} inflight {}".format(i, completers[i].requests_rec, completers[i].hiwat, brancher.inflight[i]))
