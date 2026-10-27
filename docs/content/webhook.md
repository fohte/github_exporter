---
title: "Webhook"
date: 2023-10-20T00:00:00+00:00
anchor: "webhook"
weight: 40
---

For the configuration of a webhook on your GitHub repository or your
organization you should just follow the defined steps, after that you should
receive any webhook related to the defined actions on the exporter if you have
followed the instructions from above.

If you want to configure a webhook for your organization just visit
`https://github.com/organizations/ORGANIZATION/settings/hooks/new` where you got
to replace `ORGANIZATION` with the name of your organization, after that follow
the steps from the screenshots.

If you want to configure a webhook for your repository just visit
`https://github.com/ORGANIZATION/REPOSITORY/settings/hooks/new` where you got to
replace `ORGANIZATION` with your username or organization and `REPOSITORY` with
your repository name, after that follow the steps from the screenshots.

**Payload URL** should be the endpoint where GitHub can access the exporter
through your reverse proxy, or webserver, or whatever you have configured in
front of the exporter, something like `https://exporter.example.com/github`.

**Content type** should be set to `application/json`, but in theory both formats
should be correctly parsed by the exporter.

**Secret** gets the random password you should have prepared when you have
configured the exporter, mentioned above.

**Which events would you like to trigger this webhook** got to be set to
`Let me select individual events` where you just got to check the last two items
`Workflow runs` and `Workflow jobs` (If you want to enable the workflow job collector).

After hitting the **Add webhook** button you are ready to receive first webhooks
by GitHub. It should also show that the initial test webhook have been executed
successfully.

### Workflow run attempts per branch

`github_workflow_run_attempts_per_branch_max` has `repo`, `workflow`, `event`,
and `window` labels. `repo` is the full repository name, `workflow` is the
workflow ID, and `window` is `30m` or `6h`; branch details are available in
debug logs and do not add metric series. Each window is emitted once the
exporter has recorded history for that long, so the `6h` window starts six hours
after the first start or a database loss and `history_6h_ready` turns 1. Health
condition values are `history_6h_ready`, `database_error`,
`series_limit_exceeded`, `recovery_enabled`, `recovery_permission_error`,
`recovery_rate_limit`, `recovery_pagination_truncated`, and `recovery_error`.

### Recovering failed workflow run deliveries

GitHub does not automatically retry failed webhook deliveries. The exporter can
check and redeliver failed `workflow_run` deliveries when recovery is explicitly
enabled. Recovery requires `GITHUB_EXPORTER_COLLECTOR_WORKFLOW_RUNS=true`, and
the exporter refuses to start without it. Set
`GITHUB_EXPORTER_WORKFLOW_RUN_RECOVERY_ENABLED=true` and provide the exact
configured callback URL through
`GITHUB_EXPORTER_WORKFLOW_RUN_RECOVERY_CALLBACK_URL`, for example
`https://exporter.example.invalid/github`. Recovery is disabled by default and
runs every five minutes unless `GITHUB_EXPORTER_WORKFLOW_RUN_RECOVERY_INTERVAL`
is set.

The exporter checks only repositories configured through
`GITHUB_EXPORTER_REPOS`. It redelivers deliveries only for active repository
hooks that subscribe to `workflow_run` and have the exact callback URL. A
delivery GUID is attempted at most once, and scan cursors and retry history are
stored in the configured database. `GITHUB_EXPORTER_WORKFLOW_RUN_RECOVERY_MAX_PAGES`
limits delivery pages read per matching hook per interval. A nonzero
`github_workflow_run_attempt_health{condition="recovery_pagination_truncated"}`
indicates that a delivery scan stopped at that limit with more pages remaining,
or that a repository search for a wildcard target returned incomplete results.

The GitHub token or App installation needs repository **Webhooks** permission
with read access to list hooks and deliveries. Redelivery also requires write
access. See the [repository webhook API permissions](https://docs.github.com/en/rest/repos/webhooks?apiVersion=2022-11-28#list-deliveries-for-a-repository-webhook)
and [failed delivery guidance](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries).
