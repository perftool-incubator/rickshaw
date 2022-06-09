# rickshaw documentation

## Coordination

One of rickshaw's core responsibilities is to provide coordination between the various components in the test environment.  It does this using [roadblock](https://github.com/perftool-incubator/roadblock) as a synchronization and message passing provider.  Here is a chart documenting the various synchronization points and coordination logic across the test environment:

![Coordination Logic](rickshaw-roadblocks.svg)
