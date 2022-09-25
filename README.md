We leverage the seq# for iterating the txs.
However, since the broadcast txs are eventually-ordered,
each node has its own mapping from seq# to tx digest.

To avoid data loss on crashes the Sui tx iterator binary is ran on multiple
instances across multiple regions.
Each process connects to a different RPC node.
Typically one process is connected to a db writer-replica.
This process is referred to as _leader_.

Leader determines the effective ordering of txs.
Other processes are supports which cross-reference leader's work with the output
of the RPC node they are connected to.
Each support is readily available to take over the leader role.
All iterators use the seq# to iterate over tx digests.
The leader stores those digests directly in the order it receives them into db.
The supports iterate the db as well and cross-reference the digests they got
from RPC with the ones stored in db.
Those digests observed in both are resolved and ignored.
Those digests not yet observed in db but in RPC are stored.
If a digest is not observed in db for X amount of time, the support assumes
that the leader crashed and takes over.
The amount of time X is variable per support and fine-tuned based on RPC
performance.
However, it is possible that two supports take over the leader role at the same
time.
This is ok as the leader role is purely an optimization to avoid frequent db
writes.
A supervisor job periodically queries states of all iterators and begins
apoptosis of all but one iterator which assumed the leader role.
The supervisor job periodically queries the seq# of each iterator to create
checkpoints.
When an iterator for a specific RPC node is restarted, they start iterating from
the checkpoint onwards.
