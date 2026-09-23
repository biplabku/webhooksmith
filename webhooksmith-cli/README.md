# webhooksmith-cli

CLI tool to inspect and manage [webhooksmith](https://crates.io/crates/webhooksmith) webhook queues.

```
cargo install webhooksmith-cli
```

## Usage

```
webhooksmith --db-url postgres://user:pass@localhost/mydb <COMMAND>
```

Set `WEBHOOKSMITH_DATABASE_URL` to avoid passing `--db-url` every time.

## Commands

| Command | Description |
|---|---|
| `stats` | Queue statistics (pending / delivering / failed / dead / delivered) |
| `endpoints` | List all registered endpoints with status and failure count |
| `events --status <STATUS>` | List events by status across all endpoints |
| `log <EVENT_ID>` | Full delivery attempt history for one event |
| `retry <EVENT_ID>` | Requeue a specific dead event |
| `retry-all <ENDPOINT_ID>` | Requeue all dead events for an endpoint |
| `cleanup` | Delete old delivered/dead events |

## Examples

```sh
# Show queue health
webhooksmith stats

# Find failed events
webhooksmith events --status failed --limit 50

# See why a specific delivery failed
webhooksmith log 550e8400-e29b-41d4-a716-446655440000

# Requeue everything dead for an endpoint
webhooksmith retry-all f47ac10b-58cc-4372-a567-0e02b2c3d479

# Clean up old data (default: 7 days delivered, 30 days dead)
webhooksmith cleanup --delivered-days 14 --dead-days 60
```
