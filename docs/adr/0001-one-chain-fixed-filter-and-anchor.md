# ADR-0001: One chain, fixed filter, anchored checkpoint

Status: accepted.

## Decision

One deployment indexes one expected chain ID and fixed address/topic filter. The dataset stores chain ID, trusted genesis/anchor hash, filter fingerprint, and a contiguous checkpoint `(number, hash)`. For a nonzero start, the header immediately before the first indexed block is stored as anchor. Starting above block 1 requires an independently configured expected anchor hash; a provider-reported hash alone is not trusted. A changed network/filter needs an explicit new dataset or migration/rebuild. No automatic checkpoint reset.

## Reason and consequences

The checkpoint has meaning only relative to a fixed network and filter. This prevents silent gaps when configuration changes and makes failover checks concrete. It limits first-release scope; a multi-chain service can run separate instances/databases. Tests must reject wrong chain, changed filter/anchor, and mismatched checkpoint.
