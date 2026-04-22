# Hello, Apache Iceberg Community

My name is **Alex Schiewe**, and I have been an active contributor to the Apache Iceberg
ecosystem for over two years. In that time I have contributed patches across the Java core,
PyIceberg, and the REST catalog specification, participated in design reviews on the dev mailing
list, and spoken about Iceberg internals at community meetups. Most recently I have been
deeply involved in the v3 spec work — specifically around deletion vectors and row lineage.

## Why I am forking

I am creating this personal exploration fork to do the kind of slow, deliberate research and
prototyping that does not fit cleanly inside the main ASF repository. My goals are:

1. **Prototype freely.** Some of the ideas below are half-formed; a fork lets me validate them
   before asking the community to review.
2. **Build a shared backlog.** Rather than scattering thoughts across GitHub issues and mailing
   list threads, I want a single living document I can reference in discussions.
3. **Contribute back selectively.** Anything that proves valuable will be proposed upstream
   via the normal design-then-PR process. Nothing in this fork is intended to stay private.

## My focus areas

- Streaming / Kafka integration with Iceberg (especially around commit latency and small-file
  accumulation)
- Multi-language parity (PyIceberg, Rust, Go)
- Catalog intelligence — scan planning, statistics, and federated metadata
- Operational tooling — compaction, tagging, table health dashboards

## Contact

I welcome collaboration. If any item in the backlog resonates with you, open an issue here or
find me on the Iceberg Slack (`#general`) or the dev mailing list (dev@iceberg.apache.org).

---

*This fork is not affiliated with or endorsed by the Apache Software Foundation.*
