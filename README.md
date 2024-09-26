# Introduction
This repository reproduces an unexpected behavior of aerospike
when corrupted records are encountered on disk during
rebalancing process.

After having read a bad record from disk, rebalancing process
is not progressing anymore because emitter node keeps
retransmitting an invalid migration message that is
never acknowledged by receiver side.

See comments in reproducer.py for more details.

# How to reproduce

* python3 installation (tested with python 3.11)
* docker

```bash
# prepare python environment
$ python3 -mvenv venv
$ . venv/bin/activate
$ pip install -r requirements.txt

# launch reproducer
$ python3 reproducer.py
```

It will launch two aerospike nodes in 7.1.0.2 version, simulating
disk data corruption on first node, then forming cluster
with second node to trigger rebalancing and observing migration
process being stuck.

Behavior observed on 6.3 and 7.1 branches.

NB: `post-write-cache` disabled to ease reproduction
