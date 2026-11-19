## Unreleased

### Improvements

* Added `Memberlist.SetReady`/`IsReady` and `ServiceMembers`/`NumServiceMembers`/`IsServiceable`. Membership and traffic readiness are now separate: a node stays in the member list while its initial state synchronization is in flight, but it is excluded from the cached serviceable set (and therefore request forwarding) until it marks itself ready.

### Changes

### Fixed

* Fixed intermittent "not found" failures caused by forwarding requests to a newly joined node before it finished synchronizing its state. Readiness is gossiped via the `alive` message and push/pull exchange and is served from a cached snapshot instead of being recomputed per request.

### Security
