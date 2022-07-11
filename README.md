# hermes

[![Go Report Card](https://goreportcard.com/badge/github.com/bjornaer/hermes)](https://goreportcard.com/report/github.com/bjornaer/hermes) ![tests](https://github.com/bjornaer/hermes/actions/workflows/push.yaml/badge.svg) ![GitHub last commit](https://img.shields.io/github/last-commit/bjornaer/hermes?style=plastic) ![GitHub repo size](https://img.shields.io/github/repo-size/bjornaer/hermes?style=plastic) ![Lines of code](https://img.shields.io/tokei/lines/github/bjornaer/hermes?style=plastic) ![GitHub](https://img.shields.io/github/license/bjornaer/hermes?style=flat-square) [![HitCount](https://hits.dwyl.com/bjornaer/hermes.svg?style=flat-square)](http://hits.dwyl.com/bjornaer/hermes)

**Hermes** is a distributed key-value store which guarantees data consistency through CRDT's

---
### Dowload

Please run

```go
go get "github.com/bjornaer/hermes"
```

---
### Introduction

#### CRDT
Conflict-Free Replicated Data Types (CRDTs) are data structures that power real-time collaborative applications in
distributed systems. CRDTs can be replicated across systems, they can be updated independently and concurrently
without coordination between the replicas, and it is always mathematically possible to resolve inconsistencies that
might result.

In other (more practical) words: CRDTs are a certain form of data types that when replicated across several nodes over a network achieve eventual consistency without the need for a consensus round

#### BTree
In computer science, a B-tree is a self-balancing tree data structure that maintains sorted data and allows searches, sequential access, insertions, and deletions in logarithmic time. The B-tree is a generalization of a binary search tree in that a node can have more than two children. Unlike self-balancing binary search trees, the B-tree is well suited for storage systems that read and write relatively large blocks of data, such as discs. It is commonly used in databases and file systems.

---

### Package

This package implements a `CRDT` interface that runs on top of a `BTree` structure which by itself abstracts the filesystem blocks to store data

This codebase is set to implement a DB server that allows for multiple nodes of the same DB to be run distributed and uses CRDT to derive consistency.

---
### Examples

While I haven't added examples to this DB directly you can find [examples of CRDT usage over here](https://github.com/bjornaer/crdt/examples/README.md)

---
### Run tests

To run tests

```bash
make test
```

---
### Roadmap

- [ ] Enable multiple nodes to be created 
- [ ] Have peer to peer connection working
- [ ] CI/CD
- [ ] Create Hermes-Client to acces hermes from the code
- [ ] Publish Hermes binary to Brew

---
**NOTE**
To read documentation on the public API [can be found here](https://pkg.go.dev/github.com/bjornaer/hermes)

---
### Bibliography

- [A comprehensive study of Convergent and Commutative Replicated Data Types](https://hal.inria.fr/file/index/docid/555588/filename/techreport.pdf)
- [Consistency without consensus in production systems by Peter Bourgon](https://www.youtube.com/watch?v=em9zLzM8O7c)
- [Roshi: a CRDT system for timestamped events](https://developers.soundcloud.com/blog/roshi-a-crdt-system-for-timestamped-events)
- [CRDT notes by Paul Frazee](https://github.com/pfrazee/crdt_notes)
- [Wikipedia page on CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
- [CuteDB](https://github.com/naqvijafar91/cuteDB)
