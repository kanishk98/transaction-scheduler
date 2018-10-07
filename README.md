# Transaction scheduler to guarantee concurrency control

Demonstration of a scheduling algorithm for a multi-user database system. 

## Components

- [x] Server side code to accept operation requests
- [x] bLDSF algorithm implementation for conflict-aware lock scheduling
- [x] Operation scheduling output printed appropriately and legibly
- [ ] Adding delayer to ensure serialisability of operations within transactions 
- [ ] Demonstration of consistent results 
- [ ] Ensuring recoverability
- [ ] Preventing cascading rollbacks

## Assumptions

Operations are executed in between server-side requests.

## Contributing

Please fork this repo and checkout to a new branch before implementing any modifications.
