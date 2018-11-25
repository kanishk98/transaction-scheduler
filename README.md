# Transaction scheduler to guarantee concurrency control

Demonstration of a scheduling algorithm for a multi-user database system. 

## Components

- [x] Server side code to accept operation requests
- [x] bLDSF algorithm implementation for conflict-aware lock scheduling
- [x] Operation scheduling output printed appropriately and legibly
- [x] Demonstration of consistent results 
- [x] Allowing multiple operations within same transaction in the same server request
- [ ] ~~Ensuring recoverability~~ (out of scope)
- [ ] ~~Preventing cascading rollbacks~~ (out of scope)

## Assumptions

1. Transactions are posted to the server with the understanding that an arbitrary order of execution might be followed for multiple operations within the transaction.
2. 

## References

1. [Contention-aware lock scheduling for transactional databases](https://web.eecs.umich.edu/~mozafari/php/data/uploads/pvldb_2018_sched.pdf)

## Contributing

Please fork this repo and checkout to a new branch before implementing any modifications.
