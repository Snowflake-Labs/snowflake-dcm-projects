# Snowflake DCM GitHub Actions — DEPRECATED

> ## ⛔ These actions have moved and are now DEPRECATED
>
> The DCM GitHub Actions are now officially published under
> `[snowflakedb/snowflake-actions](https://github.com/snowflakedb/snowflake-actions)`
> ([Marketplace listing](https://github.com/marketplace/actions/snowflake-actions#github-actions-for-dcm-projects)).
>
> **Please migrate to the official actions.** The versions in this folder are
> frozen and will receive no further updates. They will be removed in a future
> release.

## Migration

Replace references to the actions in this folder with the official equivalents:


| Deprecated (`Snowflake-Labs/snowflake_dcm_projects`) | Official (`snowflakedb/snowflake-actions`) |
| ---------------------------------------------------- | ------------------------------------------ |
| `actions/dcm-parse-manifest@v1`                      | `dcm/parse-manifest@v3`                    |
| `actions/dcm-connection-test@v1`                     | `dcm/connection-test@v3`                   |
| `actions/dcm-plan@v1`                                | `dcm/plan@v3`                              |
| `actions/dcm-deploy@v1`                              | `dcm/deploy@v3`                            |


For example:

```yaml
# Before
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-plan@v1

# After
- uses: snowflakedb/snowflake-actions/dcm/plan@v3
```

The inputs are compatible for `parse-manifest`, `connection-test`, and `plan`.
For `deploy`, note two differences in the official action:

- The `test-expectations` input (dynamic-table refresh + `snow dcm test`) is **not** available.
- The `test-result` output, and the `create-count` / `alter-count` / `drop-count`
outputs on `plan`, are **not** available.

See the [official DCM actions README](https://github.com/snowflakedb/snowflake-actions/blob/main/dcm/README.md)
for full, up-to-date documentation.

### Backward compatibility

Existing workflows pinned to `@v1` (e.g.
`Snowflake-Labs/snowflake_dcm_projects/actions/dcm-plan@v1`) continue to run
against the frozen `v1` tag and will keep working, but they now emit a
deprecation warning. New workflows should use the official actions above.

---
