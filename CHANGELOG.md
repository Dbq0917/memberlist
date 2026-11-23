## Unreleased

### Improvements

- Suppress duplicate membership broadcasts by logical event identity and cap
  per-message retransmissions. Repeated suspect/failure confirmations no longer
  reset propagation, and large-cluster membership changes stay linear instead
  of amplifying with cluster size.

### Changes

### Fixed

### Security
