# Multigres CLI – Vision

The **Multigres CLI** (`multigres`) is the command-line companion for managing and developing with Multigres clusters. It is designed to feel natural in the hands of developers—fast, scriptable, and ergonomic—while powerful enough to manage production-grade clusters.

Inspired by the simplicity of `gh` and `supabase`, the CLI aims to make complex workflows approachable through a single tool.

## Vision Statement

The Multigres CLI should **"make distributed Postgres feel as easy as running Postgres locally."**

A single binary that gives developers confidence when experimenting, and operators the tools to keep clusters healthy at scale.

## Tenets

- **Parity with the UI**
    
    Everything that is available in the UI can be done via the CLI.
    
- **Delightful experience**
    
    Seamless, predictable, and enjoyable interactions are paramount.
    
- **One command to cluster**
    
    Getting a cluster running on a local environment, should be as simple as:
    
    ```
    multigres up
    ```
    

## Goals

- **Unified interface**
    
    The CLI is the entry point for everything Multigres: cluster lifecycle, database management, observability, and debugging. Consistent verbs and flags keep the UX predictable.
    
- **Ergonomic for developers, powerful for operators**
    
    From local development to production management, `multigres` works across the spectrum. Friendly by default, flexible when you need it.
    
- **Scriptable and composable**
    
    Every command should play well in CI/CD pipelines and shell scripts. Output is human-friendly by default and machine-readable when requested (`--json`, `--yaml`).
    
- **First-class Go support**
    
    As with the rest of Multigres, the CLI is written in Go for speed, portability, and ease of contribution.
    

## Example Commands

### Cluster Lifecycle

```
multigres cluster init          # Create a local configuration that up can use to start the cluster
multigres cluster up            # Start local cluster
multigres cluster down          # Stop local cluster (will have flags to fully teard down)
multigres cluster status        # Show health 
```

### Topology Management

**Cells:**

```bash
multigres topology cell list
multigres topology cell get <cell>
multigres topology cell create <cell>
multigres topology cell update <cell>
multigres topology cell delete <cell>
```

**Databases:**

```bash
multigres topology db list
multigres topology db get <database>
```

### Observability

```
multigres logs cluster
multigres logs multigateway 
multigres logs multipooler poolerid --watch 
```

## Tools Comparison

Some tooling comparison brought to you by ChatGPT. I have used myself SupabaseCLI, Kubernetes, Docker Compose. I don't know the others, but adding for reference so we can also take more inspiration. 

| Tool / Platform | Key Strengths | CLI Simplicity | Rafa Notes |
| --- | --- | --- | --- |
| **Supabase CLI** | All-in-one backend (Postgres, Auth, Storage); great DX & docs; open source | **High** – one command to start full stack | I really like this. I feel what I'm proposing here is basically following what supabase did. |
| **Nhost (Hasura GraphQL)** | Instant GraphQL backend with auth/storage; strong DX; open source | **High** – CLI spins up Hasura + Postgres | I haven't used this, but looks very similar to supabase |
| **Docker Compose** | Dead-simple multi-container orchestration; minimal config; ubiquitous | **Very High** – `docker compose up` | Kind of proven pattern as well IMO. |
| **Kubernetes (Minikube/Kind/k3s)** | Standardized env; huge ecosystem; strong prod parity | **Moderate** – easy cluster start; k8s YAML/ctl adds learning |  |
| **HashiCorp Nomad** | Simple, lightweight orchestrator; single binary; optional Consul/Vault | **High** – `nomad agent -dev` + short job HCL | At first glance, looks very similar  to kubernetes cli |
| **Encore (Go backend platform)** | Automates infra from Go code; strong DX; tracing/docs/CI out-of-box | **High** – `encore run` (auto-provisions deps) |  |
| **Dapr CLI (sidecar runtime)** | Simplifies svc-to-svc, state, pub/sub; language-neutral; CNCF project | **High** – `dapr init` / `dapr run` |  |

## Cluster Bootstrap

When starting a Multigres cluster, the CLI needs to bootstrap the system from scratch. This bootstrap involves three categories of work:

1. **External dependencies:** bringing up supporting systems such as `etcd`.
2. **Topology initialization:** applying the initial configuration so the cluster has a known starting state.
3. **Core Multigres components:** starting the bare minimum services, such as `multigateway` and `multiorch`, so the cluster can coordinate and route requests.

Later, when users request a new database, the system must also provision additional components. For example, bringing up a new Postgres instance alongside a `multipooler`.

To handle both bootstrap and on-demand provisioning in a consistent way, Multigres defines an explicit contract: the **Provisioner API**. The CLI is a **consumer** of this API. Instead of hard-coding platform-specific scripts, the CLI delegates to a provisioner that knows how to start and stop Multigres components on the chosen runtime.

### Provisioner Flavors for MVP

- **Local:** Optimized for Multigres developers. Assumes dependencies are available on PATH (etcd, Postgres, and Multigres binaries). The local provisioner launches these processes directly and manages their lifecycle. Best for iterating on code and debugging locally.
- **Docker:** Optimized for evaluation and quick starts. Uses pinned Docker images to bring a cluster up with minimal host setup. This mirrors the approach used by the Supabase CLI and is ideal for users who want a fast, reproducible "try it on my laptop" path.
- **Kubernetes Operator (Production):** Recommended for production. We likely won't expose full up/down flows for production clusters in the CLI; those actions belong to the operator or your platform automation. The CLI can still attach to an existing production cluster (similar to the Supabase CLI) for day-to-day admin, observability, and maintenance, while destructive lifecycle actions remain restricted. Multigres will continue to use the Kubernetes provisioner path to add components on demand (e.g., when provisioning a new database).

### Bootstrap steps

The following are the steps that need to happen to bootstrap a multigres cluster:

1. Provision etcd. 
2. Create an initial cell in the global topo. 
3. Create Database.

The following diagram, illustrates the full bootstrap: [figjam](https://www.figma.com/board/WLQsyCchb25623GITzFNMp/Untitled?node-id=0-1&p=f&t=4NHpeV6djlcuAtZ9-0)

## **Design Guideline: CLI as a Thin Layer**

Our philosophy is that everything available in the Admin UI should also be accessible through the CLI. This principle should guide the design of new commands.

From an implementation perspective, the CLI should act as a thin layer over the Admin API. In other words, we don't implement admin commands twice. Instead, we expose functionality through an RPC API, and the CLI consumes that API.

This approach matters for two reasons:

- **Simplicity:** It keeps the CLI implementation lightweight and maintainable.
- **Production readiness:** In production, CLI users won't have direct access to Multigres components. They must connect through the Admin API, which enforces authentication and authorization. The Admin API, in turn, has the necessary privileges to reach other components.

<aside>
💡

In our initial bootstrap work, we made a direct call to the topology service as a shortcut. As we add more commands, we should transition to consistently using the Admin API instead.

</aside>