# Deploy GraphLake

The v1.1.2 release provides Linux packages for `aarch64` (ARM64) and `x86_64` (AMD64). Debian 12 with systemd is the installation baseline. For each host, keep the architecture-specific archive, its SHA-256 file, and the `BUILD-INFO` stored inside the archive.

## Standalone service

Download both files for the host architecture from the [v1.1.2 release](https://github.com/SamuelSupe/graphlake/releases/tag/v1.1.2). Run the commands below in the download directory. Set `artifact` to the actual archive filename without `.tar.gz`.

```sh
artifact=graphlake-v1.1.2-SOURCE_SHA12-linux-$(uname -m)
sha256sum -c "$artifact.tar.gz.sha256"
release_dir="/opt/graphlake/releases/${artifact#graphlake-}"

if ! id graphlake >/dev/null 2>&1; then
  sudo useradd --system --user-group --home-dir /var/lib/graphlake \
    --shell /usr/sbin/nologin graphlake
fi
sudo install -d -m 0755 /opt/graphlake/releases
sudo install -d -m 0700 -o graphlake -g graphlake \
  /var/lib/graphlake /var/lib/graphlake/catalog
sudo install -d -m 0750 /etc/graphlake
sudo mkdir -- "$release_dir"
sudo tar -xzf "$artifact.tar.gz" --strip-components=1 -C "$release_dir"
sudo ln -s "$release_dir" /opt/graphlake/current.next
sudo mv -Tf /opt/graphlake/current.next /opt/graphlake/current

sudo install -m 0644 /opt/graphlake/current/graphlake.service \
  /etc/systemd/system/graphlake.service
sudo -u graphlake /opt/graphlake/current/bin/graphlake-admin \
  --catalog-dir /var/lib/graphlake/catalog auth init
sudo systemctl daemon-reload
sudo systemctl enable --now graphlake

curl -fsS http://127.0.0.1:7337/healthz
curl -fsS http://127.0.0.1:7337/readyz
```

Run `auth init` once per catalog. Keep the existing credentials when migrating a catalog. Place models and their relative data files under `/var/lib/graphlake/catalog` and give the service user access. Store source credentials referenced by a model in the root-owned `/etc/graphlake/graphlake.env` with mode `0600`.

The service listens on loopback. For remote browser access, use an SSH tunnel (`ssh -N -L 7337:127.0.0.1:7337 graph-host`) or configure an HTTPS reverse proxy. Set `--public-url` to the exact browser origin. The [service and client reference](../deploy/README.md) covers proxy configuration, Bolt, Gremlin, and role-based service accounts.

## Distributed deployment and failover

Run Workers on private endpoints and configure the Coordinator with the cluster YAML. All nodes should use the same GraphLake build and have access to the data referenced by the published model.

For Coordinator failover, provision an etcd quorum and S3-compatible shared control storage. Use a TLS ingress that probes the elected Coordinator and the configured client protocols. The [cluster reference](../deploy/cluster.md#coordinator-故障接管) documents the startup flags, ingress configuration, and maintenance order.

## Monitoring and recovery

- Use `/healthz` to confirm the build and `/readyz` for service readiness. Run published-model probes and known-result queries through the client-facing entry point. [Monitoring reference](../deploy/monitoring.md)
- Load the shipped Prometheus rules and configure the monitoring process for each role. Enable OTLP metrics, logs, and traces through Runtime Settings or a process-local telemetry YAML file. [Telemetry reference](../deploy/README.md#opentelemetry-上报)
- Schedule control-state and local-table export and coordinate any PostgreSQL and external Parquet snapshots at a known recovery point. Use the recovery manifest to bind the control archive, the PostgreSQL archive and relations, the Parquet objects, and expected query results. Synchronize the control JSON and its matching `local-<id>` directory, when present, to an independent site.
- Prepare a fresh target namespace, an empty PostgreSQL database when the manifest includes one, and the read-only off-site backup inbox. Open the target's HTTPS `/recovery` page, choose a checkpoint, follow import progress, and run acceptance. Cutover is permitted after the expected query succeeds and the actual HTTPS business endpoint is ready. [Restore configuration and manifest format](../deploy/cluster.md#绑定恢复点并开放入口)

## Upgrade and rollback

Keep the previous archive and the existing `/opt/graphlake/current` target. Back up the complete catalog directory, credentials, and any externally stored datasets before installation.

For standalone upgrades, include `catalog/.graphlake-local/` in the offline catalog backup. The HA backup exports the committed local-table files and restores them through the recovery page or CLI. Restore the pre-upgrade catalog when rolling back the service. Filesystem replicas use the same absolute paths on Coordinators and Workers. [S3-managed local tables](../deploy/cluster.md#s3-托管本地表) use logical references and can run on Workers without a shared source mount. [Local table storage and recovery](local-tables.zh-CN.md#存储与部署)

Stop the standalone service, unpack the new release into a separate directory, update the systemd unit if needed, and atomically switch `current`. Start the service and verify the reported build, the published model revision, and a known-result query. To roll back, stop the service, switch `current` and the unit back to the previous version, and restore the matching saved catalog if its state changed. [Detailed upgrade procedure](../deploy/README.md#升级回退与恢复)

For an HA rollout, freeze local-table writes and take a checkpoint first. Upgrade Workers one at a time and compare the same published revision after each replacement. Start the candidate Coordinator before handing off leadership. Once all readers are upgraded, create and fully load a managed copy, compare the old and new results, and publish its `local-table://` reference. Keep the original table files and pre-upgrade catalog together for rollback. After activating managed writes, rehearse restoring that checkpoint in a clean namespace and account for changes made after the checkpoint. [Rehearsal and acceptance record](production-optimization-s7.zh-CN.md)

Use the live graph for SCD2 queries that evaluate date or timestamp validity at the current time. For historical CSR queries, build the generation with an explicit `graph_options.snapshot_time`. Rebuild older CSR generations that use business-integer validity without a current flag so the manifest records their clock behavior. After rolling back a model revision, rebuild its CSR if the retired pointer has been collected. [CSR query and retention reference](../deploy/cluster.md)

## Build a package from source

GitHub runs the [regression workflow](../.github/workflows/regression.yml) for pull requests and pushes to main. The [release workflow](../.github/workflows/release-build.yml) records the same commit, lockfile and fixture digests before producing Debian 12 ARM64 and AMD64 candidates. It starts each extracted archive to check its binaries, web assets, local data sync and crash recovery. Final release artifacts require the rolling-upgrade, rollback, soak, pinned-generation recovery and language binding gates. The scheduled run retains candidate evidence. Repository branch protection controls which checks a pull request must pass before merging.

To run the service checks on a Linux test machine, use the fixture containers and the same test entry point.

```sh
docker compose -f tests/production-compose.yml up -d --wait
bash tests/production-services.sh
docker compose -f tests/production-compose.yml down -v
```

On Debian 12, install Rust 1.95 or later, a native C/C++ toolchain, binutils, LLD, Git, and Node.js 24. From a clean checkout, run:

```sh
RUSTFLAGS="-C link-arg=-fuse-ld=lld" sh deploy/package.sh v1.1.2 dist
```

Run this on each target architecture or provide the corresponding cross linker and Rust target, then set `GRAPHLAKE_BUILD_TARGET` to `aarch64-unknown-linux-gnu` or `x86_64-unknown-linux-gnu`. The script checks the architecture, embedded source version, and glibc baseline and writes an accompanying source archive and checksums.
