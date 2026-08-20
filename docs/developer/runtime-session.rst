RuntimeSession execution ownership
==================================

RuntimeSession is the node-local execution owner for one allocation attempt on
one node. It exists so restarting or compatibly upgrading ``spurd`` does not
make a running allocation orphaned, double-book node resources, or lose its
signal and completion authority.

Authority
---------

``spurctld`` and Raft remain the authority for jobs, placement, global resource
ownership, and the current allocation generation. ``spurd`` authenticates and
coordinates node-local requests. A RuntimeSession owns only live execution
state: process trees, cgroups, logical steps, PTYs and I/O, PMIx namespaces,
hooks, signals, and observed exit status.

RuntimeSession must never choose placement or accept stale execution state as
truth. Every reconnect and report is fenced by the controller's current job
attempt. A superseded runtime is killed rather than allowed to report a result
for a replacement attempt.

Local persistence
-----------------

Each runtime has a private directory under ``<state_dir>/runtime`` named
``<job-id>.<attempt>``. The descriptor contains only the identity and recovery
anchors needed to locate and fence the live owner:

* format and local-RPC protocol versions;
* job id and controller attempt;
* runtime PID plus kernel process start time, preventing PID reuse;
* private Unix-socket location and capability; and
* cgroup location.

The descriptor is not a copy of task state. A restarted agent queries the live
runtime for its state instead of reconstructing process trees from ``/proc``.
An append-only obligation WAL records only crash-sensitive boundaries: observed
exit, epilog completion, acknowledged completion report, and resource release.

Failure contracts
-----------------

* A ``spurd`` crash, restart, or compatible upgrade leaves RuntimeSession and
  its children running. The replacement agent discovers the descriptor,
  authenticates to the socket, verifies the attempt, and reconnects.
* A RuntimeSession crash is fail-closed in v1: fence its cgroup and fail or
  requeue its attempt. Process adoption is not a recovery mechanism.
* A partial multi-node reconnect is unhealthy. The controller applies the
  allocation's explicit fail/requeue policy; a subset of runtimes must not
  silently continue as a healthy allocation.
* A client disconnect does not transfer execution ownership to the client.
  Output attachment is resumable only while the runtime's bounded live buffer
  retains the requested offset.

Local protocol
--------------

The Unix-socket protocol is versioned and capability-authenticated. Its current
operations are ``Hello``, ``QueryState``, ``LaunchStep``, ``SignalStep``,
``LaunchPty``, ``WritePty``, ``ReadPty``, ``ResizePty``, ``SignalPty``,
``BeginTeardown``, and ``Shutdown``. ``ReadPty`` uses a monotonic output offset
and a bounded one-mebibyte live buffer, so an agent or client reconnect can
resume output without becoming the owner of the terminal. An N/N-1 compatible
protocol is required for one release during rolling upgrades. Incompatible
runtimes require draining a node before the agent upgrade.

Rollout
-------

RuntimeSession is introduced behind a rollout gate. The initial cohort covers
selected batch, step, PTY, and PMIx canaries. The universal target is exactly
one RuntimeSession per ``(job, attempt, node)``; logical steps are children,
not per-step supervisors. Promotion requires measured restart recovery,
protocol compatibility, supervisor reliability, memory/process overhead,
launch latency, and N/N-1 upgrade behavior.

Current development gate
------------------------

Set ``SPUR_RUNTIME_SESSION=1`` in the ``spurd`` service environment to route
the initial plain-batch cohort and interactive allocation attachments through
RuntimeSession. The runtime owns interactive child PTYs, terminal input,
resizes, foreground signals, and replayable output; an agent restart no longer
sends a hangup to an attached terminal. Primary PTY batch launches, container,
GPU, and PMIx workloads remain rejected until their full execution contracts
are implemented. ``SPUR_RUNTIME_STATE_DIR`` overrides the default
``/var/spool/spur`` local runtime directory for isolated testing.
