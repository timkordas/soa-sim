import random
import functools
import simpy

from SimComponents import RequestGenerator, RequestSink, RequestCompleter, RandomBrancher

# Ten generators, going into ten priority-queue branchers, with 15 shared completers
if __name__ == '__main__':
    # random-variables for distribution of requests
    # inter-request-time distribution
    idist = functools.partial(random.expovariate, 20.0)
    # request cost distribution
    mean_cost = 100
    cdist = functools.partial(random.expovariate, 1.0/mean_cost)

    # Create the SimPy environment. This is the thing that runs the simulation.
    env = simpy.Environment()

    # Create the packet generators and sink
    sink = RequestSink(env, debug=True, rec_arrivals=True, selector=None)

    n_completers = 15
    completers = [None]*n_completers
    for i in range(n_completers):
        completers[i] = RequestCompleter(env)
        completers[i].out = sink

    completers[0].extra_delay_factor = 2.5

    generators = []
    branchers = []
    count = 10
    for i in range(count):
        generators.append(RequestGenerator(env, "genA"+str(i), idist, cdist, initial_delay=50, finish=3000))
        branchers.append(PQueueBrancher(env, completers))
        generators[i].out = branchers[i]

    env.run(until=45000)

    for i in range(n_completers):
        print ("{} reqs: {} hiwater {} inflight {}".format(i, completers[i].requests_rec, completers[i].hiwat, branchers[0].inflight[i]))
