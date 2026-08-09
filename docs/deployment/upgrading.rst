Upgrading Spur
==============

This page covers upgrading Spur binaries, whether on a single host or across a whole
cluster. It describes the ``spur self-update`` self-updater, re-running ``install.sh``,
and the Ansible playbooks that upgrade a running cluster with or without an outage.

.. note::

   There are two upgrade scopes: **upgrading the binaries on a single host** and
   **upgrading a whole cluster**. There is **no in-process hot-swap** that preserves
   running jobs inside a single daemon process. ``spur self-update`` only swaps the
   binaries on disk — it does **not** restart the daemons. The drain-aware, jobs-preserving
   cluster path is the Ansible ``rolling_upgrade.yml`` playbook (see below).

Single-Host: ``spur self-update``
---------------------------------

``spur self-update`` downloads the latest release and replaces the ``spur``, ``spurctld``,
and ``spurd`` binaries on the current host. It is a single-host convenience: it does not
restart daemons and gives no drain or quorum protection, so it is not a substitute for the
cluster playbooks below.

Check whether an update is available:

.. code-block:: bash

   spur version --check

.. code-block:: text

   update available: 0.3.0 → v0.3.1
   Run `spur self-update` to install.

Install the update:

.. code-block:: bash

   spur self-update

``spur update`` is an alias for ``spur self-update``. Add ``--nightly`` to pull from the
nightly channel instead of the latest stable release:

.. code-block:: bash

   spur self-update --nightly

The updater downloads the release tarball, verifies it against its published SHA256
checksum, then replaces each binary atomically: the current binary is renamed to
``<name>.spur-old`` as a backup, the new binary is copied into place, and the backups are
deleted on success. If any copy fails, the ``.spur-old`` backup is restored. The install
directory is auto-detected as wherever the running ``spur`` binary already lives.

After a successful update the CLI prints:

.. code-block:: text

   Updated spur to v0.3.1
   Note: Restart running daemons (spurctld, spurd) to use the new version.

.. warning::

   ``spur self-update`` never restarts daemons. Running ``spurctld`` and ``spurd``
   processes keep executing the old binary until you restart them yourself:

   .. code-block:: bash

      sudo systemctl restart spurctld spurd

The ``[update]`` config block
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The optional ``[update]`` block in ``spur.conf`` controls the daemon startup update check.
Its fields are:

.. list-table::
   :header-rows: 1

   * - Field
     - Default
     - Effect
   * - ``check_on_startup``
     - ``true``
     - Check the GitHub releases API when the daemon starts and log if an update exists.
   * - ``auto_update``
     - ``false``
     - Download and install an available update automatically. Even when ``true``, the
       daemon is **never** auto-restarted.
   * - ``channel``
     - ``"stable"``
     - Release channel to check: ``"stable"`` or ``"nightly"``.
   * - ``cache_dir``
     - ``"/var/cache/spur"``
     - Directory for the update-check cache (1-hour TTL).

.. note::

   Even with ``auto_update = true``, a new binary on disk does not take effect until the
   daemon is restarted. The config block applies to ``spurctld``; ``spurd`` never
   auto-installs updates.

See :doc:`/admin-guide/configuration` for the full ``[update]`` field reference.

Single-Host: Re-running ``install.sh``
--------------------------------------

Re-running the one-line installer upgrades an existing single-host install in place. It
downloads the requested release, verifies its checksum, and copies the binaries over the
install directory (default ``~/.local/bin``):

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash

Pass ``nightly`` or a pinned ``vX.Y.Z`` to select a specific release, or set
``INSTALL_DIR`` to install elsewhere:

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash -s -- nightly
   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash -s -- v0.3.1

Like ``spur self-update``, ``install.sh`` does not manage systemd units or restart
daemons. Restart ``spurctld`` and ``spurd`` yourself after re-installing.

Cluster Upgrades with Ansible
-----------------------------

For a multi-node cluster, the Ansible toolkit is the recommended upgrade path. Two
playbooks are supported; both reuse the same install, config, and health-check roles as
``deploy.yml``, so their behavior stays consistent.

Rebuild all three binaries from the same source tree together — they share a Raft
write-ahead-log schema, and mixing binaries from different builds can leave a controller
unable to parse a log written by a differently-versioned peer:

.. code-block:: bash

   cargo build --release -p spur-cli -p spurctld -p spurd

.. warning::

   A release that adds Raft WAL operation variants requires a coordinated upgrade of every
   controller. Drain work first, stop controller writes, replace the complete controller set,
   and restart it together. An upgraded leader must not commit a new operation while an older
   peer that cannot decode it remains in the quorum.

Binaries roll out by content, not version string: Ansible compares checksums, so an
unchanged re-run is a near no-op.

Full convergence (``deploy.yml``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``deploy.yml`` is the simplest upgrade: it re-installs binaries and restarts every daemon
on every host in one play. This causes a **brief cluster-wide outage** — in-flight jobs are
disrupted, and in an HA setup all controllers bounce together, briefly losing the Raft
leader. It is non-destructive by default (state is preserved). Use it for topology changes
or when a short blip is acceptable:

.. code-block:: bash

   ansible-playbook playbooks/deploy.yml -i inventory/hosts.ini -e spur_binary_src=/path/to/target/release

.. note::

   Spur 0.3.0 has no online Raft membership change. Adding, removing, or reordering a
   **controller** fails early unless you also pass ``-e spur_wipe_state=true`` (a Raft
   reinit that wipes state). Compute agents are not Raft members, so they can be added or
   removed freely without a wipe.

Rolling upgrade (``rolling_upgrade.yml``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``rolling_upgrade.yml`` is the no-full-outage path for releases whose notes confirm that their
Raft WAL is readable by the previous controller version. It upgrades one host at a time so the
cluster keeps scheduling and running jobs throughout.

.. code-block:: bash

   ansible-playbook playbooks/rolling_upgrade.yml -i inventory/hosts.ini -e spur_binary_src=/path/to/target/release

The playbook proceeds in order:

1. **Guard rails.** Abort if ``spur_wipe_state=true`` (never wipe Raft mid-upgrade), if
   ``spur_transport=wireguard`` (not yet supported by this playbook), or if the cluster is
   not already healthy (``spur nodes`` must return success).
2. **Upgrade controllers one at a time** (``serial: 1``, no failures tolerated), preserving
   Raft quorum. Each controller's binaries are force-reinstalled and the daemon restarted;
   the "wait for Raft leader" step is the health gate before moving to the next controller.
   The existing ``spur.conf`` is preserved unless you pass ``-e spur_overwrite_conf=true``.
3. **Drain and upgrade agents in batches.** For each agent: drain it from the controller
   (``spur node drain <node> --reason "ansible rolling upgrade"``), wait until its state is
   ``DRAINED`` or ``DOWN`` (running jobs finish first — drain never force-kills), swap the
   binary, restart ``spurd``, wait for the node to re-register, then resume it
   (``scontrol update NodeName=<node> State=RESUME``).
4. **Verify.** Submit a real test job to confirm the upgraded cluster schedules work.

Do not use the serial controller phase across a WAL-variant boundary. Use a maintenance window
and coordinated controller restart instead, then continue with the drained agent rollout.

The rolling upgrade is controlled with these ``-e`` flags:

.. list-table::
   :header-rows: 1

   * - Flag
     - Default
     - Effect
   * - ``spur_binary_src=<dir>``
     - *(unset)*
     - Directory of pre-built ``spur``/``spurctld``/``spurd`` binaries to roll out.
       Unset → install the published release via ``install.sh`` (``spur_version``).
   * - ``spur_rolling_batch_size=<N>``
     - ``1``
     - Agents upgraded per batch. Controllers are always upgraded one at a time regardless.
   * - ``spur_ignore_unreachable_agents=true``
     - ``false``
     - Skip agents unreachable over SSH instead of aborting.
   * - ``spur_skip_busy_agents=true``
     - ``false``
     - Leave a still-busy node on its current binary and continue, rather than aborting.
   * - ``spur_force_upgrade_busy_agents=true``
     - ``false``
     - Kill running jobs and containers on a busy node and upgrade it anyway. Affected jobs
       are marked ``NODE_FAIL``.
   * - ``spur_overwrite_conf=true``
     - ``false``
     - Re-render ``spur.conf`` from the Ansible template. Needed only when inventory
       variables changed; otherwise the existing config is preserved.

.. note::

   A larger ``spur_rolling_batch_size`` upgrades faster but drains more capacity at once. A
   single-controller cluster still has a short outage while its own controller restarts —
   true zero-downtime requires an HA quorum of 3 or more controllers.

Agent restart recovery
~~~~~~~~~~~~~~~~~~~~~~

After every agent runs a version with recovery manifests, restarting ``spurd`` preserves a
top-level batch job when it uses file-backed standard I/O and does not use PMIx. The new agent
re-adopts the recorded PID, cgroup, exact CPU/GPU allocation, command generation, and pending
completion obligations before it registers with the controller.

Drain agents before the first upgrade to this manifest format. Older agents did not record the
state needed for safe adoption. The upgraded agent can settle an old workload only when it can
discover a surviving Spur cgroup; a non-root or otherwise uncgrouped process is not discoverable
and could otherwise outlive the agent while its resources are advertised as free.

PTY sessions, PMIx jobs, standalone allocations, active job steps, and process-only recovery
evidence are not transparently adopted. If any of that evidence is live, recovery fails closed
and settles the whole allocation before registration; an otherwise eligible file-backed batch
process is not re-adopted separately.

Spur does not yet run a ``slurmstepd``-style per-allocation supervisor. Recovery manifests and
the batch shell's exit-status sentinel cover the supported case; preserving terminal ownership,
file descriptors, a PMIx server, and exact wait status requires a future supervisor design.

Safe Upgrade Order
~~~~~~~~~~~~~~~~~~~

Follow this order for any cluster upgrade:

1. **Rebuild all three binaries together** from the same source tree — they share a Raft
   WAL schema and must stay version-matched.
2. **Drain before the first recovery-manifest upgrade or any WAL-variant boundary.** Older
   agents cannot describe uncgrouped work, and older controllers cannot decode new operations.
3. **Upgrade controllers before agents.** Use a coordinated all-controller restart for a new
   WAL variant. A serial controller rollout is only safe when both versions decode the same WAL.
4. **Upgrade the agents.** Old agents are temporarily ineligible for new scheduling after the
   controllers move to durable allocation inventory. Capacity returns as upgraded agents
   re-register; the rolling playbook drains each node before replacing its binary, and a running
   job blocks the swap unless you force it.
5. **Never wipe state during an upgrade.** Keep the default ``spur_wipe_state=false``.
   Wiping resets the Raft job-id counter and destroys accounting history.
6. **HA membership is fixed at init** in Spur 0.3.0. Changing *which* hosts are controllers
   requires ``deploy.yml -e spur_wipe_state=true``.

See Also
--------

- :doc:`ansible`
- :doc:`uninstalling`
- :doc:`/admin-guide/configuration`
