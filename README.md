# Transaction scheduler to guarantee consistency

Demonstration of a concurrency-mindful scheduling algorithm for a multi-user database system. 

## Components

- [x] Server side code to accept operation requests
- [x] bLDSF algorithm implementation for conflict-aware lock scheduling
- [x] Operation scheduling output printed appropriately and legibly
- [ ] ~~Adding delayer to ensure serialisability of operations within transactions~~
- [x] Demonstration of consistent results
- [x] Ensuring no deadlocks
- [ ] Preventing starvation after removal of Assumption 1
- [ ] Ensuring recoverability
- [ ] Preventing cascading rollbacks

## Assumptions

1. Operations are executed in between server-side requests.
2. The same server request does not include conflicting operations from the same transaction.

## References

1. [Contention-aware lock scheduling for transactional databases](https://web.eecs.umich.edu/~mozafari/php/data/uploads/pvldb_2018_sched.pdf)

## Contributing

Please fork this repo and checkout to a new branch before implementing any modifications.
