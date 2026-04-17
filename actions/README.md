# Snowflake DCM GitHub Actions

> ⚠️ These actions are provided as-is for evaluation purposes.
> They are not officially supported by Snowflake.
> Breaking changes may occur at any time.
> Use at your own risk.

A set of **reusable composite GitHub Actions** for automating [Snowflake DCM Projects](https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-overview) pipelines. Each action handles one step of the lifecycle, and you can reference them from your own workflows to build end-to-end CI/CD pipelines.

To use an action in your workflow, reference it with:

```yaml
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/<action-name>@v1
```

For complete, ready-to-use workflow examples that compose these actions, see the [Sample Workflows](../GitHub_workflows/README.md).

## Actions

| Action | Description |
|--------|-------------|
| [`dcm-parse-manifest`](#dcm-parse-manifest) | Parse `manifest.yml` and output target names as a JSON array for matrix strategies |
| [`dcm-connection-test`](#dcm-connection-test) | Test Snowflake connectivity, validate role match, check project status |
| [`dcm-plan`](#dcm-plan) | Run `snow dcm plan`, summarize the changeset, upload artifacts |
| [`dcm-deploy`](#dcm-deploy) | Deploy with optional drop detection, DT refresh, and expectation testing |

## Authentication

All actions authenticate to Snowflake using the [`snowflakedb/snowflake-cli-action@v2.0`](https://github.com/snowflakedb/snowflake-cli-action) with **OIDC** (OpenID Connect) enabled. Each action calls this internally — you do not need to add a separate authentication step in your workflow.

**OIDC is the recommended approach.** It uses GitHub's built-in identity tokens via Workload Identity Federation so no passwords or private keys need to be stored as secrets. To use OIDC:

1. Create a Snowflake service user with OIDC workload identity. The `SUBJECT` must match exactly what GitHub sends — case-sensitive, no wildcards. Since these actions use GitHub Environments, use the environment-based subject format:

   ```sql
   CREATE USER SVC_GITHUB_ACTIONS
     TYPE = SERVICE
     DEFAULT_ROLE = 'PUBLIC'
     COMMENT = 'GitHub Actions service user for CI/CD via OIDC'
     WORKLOAD_IDENTITY = (
       TYPE = OIDC
       ISSUER = 'https://token.actions.githubusercontent.com'
       SUBJECT = 'repo:<owner>/<repo>:environment:<env_name>'
     );
   ```

   Replace `<owner>/<repo>` with your GitHub repository and `<env_name>` with the GitHub Environment name (e.g. `DCM_STAGE`). If you have multiple environments, you will need a separate service user per environment or use [subject claim customization](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/about-security-hardening-with-openid-connect#customizing-the-subject-claims).

2. Grant the service user the role specified as `project_owner` in your manifest:

   ```sql
   GRANT ROLE MY_DEPLOYER_ROLE TO USER SVC_GITHUB_ACTIONS;
   ```

3. Create a GitHub Environment for each DCM target (e.g. `DCM_STAGE`, `DCM_PROD_US`) — the environment name must match the `SUBJECT` claim
4. Set `SNOWFLAKE_USER` in the workflow `env` block to the service user name
5. Grant the workflow `id-token: write` permission

**PAT and key-pair authentication** are also supported. If you cannot use OIDC, set the appropriate environment variables in your workflow before calling the actions:

- **PAT / Password:** Set `SNOWFLAKE_PASSWORD` in your workflow `env` block (sourced from a secret)
- **Key-pair:** Set `SNOWFLAKE_PRIVATE_KEY_RAW` and `SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT` in your workflow `env` block

## Prerequisites

All actions require:

- A **GitHub Environment** matching the DCM target name (e.g. `DCM_STAGE`, `DCM_PROD_US`)
- **Workflow permissions**:

```yaml
permissions:
  id-token: write
  contents: read
```

When using `comment-on-pr: "true"` on `dcm-plan` or `dcm-deploy`, also add:

```yaml
permissions:
  id-token: write
  contents: read
  pull-requests: write
```

---

## dcm-parse-manifest

Reads a DCM `manifest.yml` and outputs the list of target names as a JSON array, ready to feed into a GitHub Actions matrix strategy. This is useful for dynamically running jobs across all targets without hardcoding them.

```yaml
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-parse-manifest@v1
  id: manifest
  with:
    project-path: my-dcm-project/
```

### Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `project-path` | yes | Path to the DCM project directory (containing `manifest.yml`) |

### Outputs

| Output | Description |
|--------|-------------|
| `targets` | JSON array of target names (e.g. `["DCM_STAGE","DCM_PROD_US"]`) |

### Example: Dynamic matrix strategy

```yaml
jobs:
  parse:
    runs-on: ubuntu-latest
    outputs:
      targets: ${{ steps.manifest.outputs.targets }}
    steps:
      - uses: actions/checkout@v4
      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-parse-manifest@v1
        id: manifest
        with:
          project-path: my-dcm-project/

  deploy:
    needs: parse
    strategy:
      matrix:
        target: ${{ fromJson(needs.parse.outputs.targets) }}
    runs-on: ubuntu-latest
    environment: ${{ matrix.target }}
    steps:
      - uses: actions/checkout@v4
      # ... use other dcm actions with target: ${{ matrix.target }}
```

---

## dcm-connection-test

Tests the Snowflake connection for a target, validates that the connection role matches the manifest `project_owner`, and checks whether the DCM project already exists.

```yaml
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-connection-test@v1
  with:
    target: DCM_STAGE
    project-path: my-dcm-project/
    snowflake-user: ${{ env.SNOWFLAKE_USER }}
```

### Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `target` | yes | DCM target name from `manifest.yml` |
| `project-path` | yes | Path to the DCM project directory |
| `snowflake-user` | yes | Snowflake username for authentication |

### Outputs

| Output | Description |
|--------|-------------|
| `result` | `success` or `failure` |
| `connection-account` | Snowflake account from the connection test |
| `connection-role` | Role used by the connection |
| `project-exists` | `true` or `false` |

---

## dcm-plan

Runs `snow dcm plan` against a target, writes a changeset summary (CREATE / ALTER / DROP counts by object domain) to the GitHub Step Summary, and uploads the plan output as an artifact.

```yaml
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-plan@v1
  with:
    target: DCM_STAGE
    project-path: my-dcm-project/
    snowflake-user: ${{ env.SNOWFLAKE_USER }}
    comment-on-pr: "true"
```

### Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `target` | yes | | DCM target name from `manifest.yml` |
| `project-path` | yes | | Path to the DCM project directory |
| `snowflake-user` | yes | | Snowflake username for authentication |
| `create-if-not-exists` | no | `true` | Run `snow dcm create --if-not-exists` before planning |
| `comment-on-pr` | no | `false` | Post the plan summary as a comment on the associated PR |

### Outputs

| Output | Description |
|--------|-------------|
| `result` | `success` or `failure` |
| `plan-file` | Path to `plan_result.json` |
| `create-count` | Number of CREATE operations |
| `alter-count` | Number of ALTER operations |
| `drop-count` | Number of DROP operations |

---

## dcm-deploy

Deploys the DCM project to a target. Optionally checks for destructive DROP operations before deploying and can run dynamic table refresh + data quality expectation tests after deployment.

The `dcm-plan` action **must** run before this action in the same job -- it produces the `out/plan/plan_result.json` file used for drop detection.

```yaml
- uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-deploy@v1
  with:
    target: DCM_STAGE
    project-path: my-dcm-project/
    snowflake-user: ${{ env.SNOWFLAKE_USER }}
    allow-drops: "false"
    test-expectations: "true"
    comment-on-pr: "true"
```

### Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `target` | yes | | DCM target name from `manifest.yml` |
| `project-path` | yes | | Path to the DCM project directory |
| `snowflake-user` | yes | | Snowflake username for authentication |
| `allow-drops` | no | `false` | Set to `true` to skip destructive drop detection |
| `test-expectations` | no | `false` | Refresh dynamic tables and run `snow dcm test` after deploy |
| `comment-on-pr` | no | `false` | Post a deploy summary as a comment on the associated PR |
| `post-scripts-path` | no | `""` | Relative path (from project-path) to a directory of `.sql` files to run after deploy, before refresh/test. Files are executed alphabetically with Jinja templating using manifest variables. |

### Outputs

| Output | Description |
|--------|-------------|
| `deploy-result` | `success` or `failure` |
| `test-result` | `success`, `failure`, or `skipped` |

---

## Full Example Workflow

A complete STAGE + PROD pipeline with PR comments:

```yaml
name: DCM Deploy

on:
  push:
    branches: [main]
    paths: ['my-dcm-project/**']

env:
  DCM_PROJECT_PATH: my-dcm-project/
  SNOWFLAKE_USER: SVC_GITHUB_ACTIONS

jobs:
  # ---- STAGE ----
  stage:
    runs-on: ubuntu-latest
    environment: DCM_STAGE
    permissions:
      id-token: write
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v4

      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-connection-test@v1
        with:
          target: DCM_STAGE
          project-path: ${{ env.DCM_PROJECT_PATH }}
          snowflake-user: ${{ env.SNOWFLAKE_USER }}

      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-plan@v1
        with:
          target: DCM_STAGE
          project-path: ${{ env.DCM_PROJECT_PATH }}
          snowflake-user: ${{ env.SNOWFLAKE_USER }}
          comment-on-pr: "true"

      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-deploy@v1
        with:
          target: DCM_STAGE
          project-path: ${{ env.DCM_PROJECT_PATH }}
          snowflake-user: ${{ env.SNOWFLAKE_USER }}
          test-expectations: "true"
          comment-on-pr: "true"

  # ---- PROD ----
  prod:
    needs: stage
    runs-on: ubuntu-latest
    environment: DCM_PROD_US
    permissions:
      id-token: write
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v4

      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-plan@v1
        with:
          target: DCM_PROD_US
          project-path: ${{ env.DCM_PROJECT_PATH }}
          snowflake-user: ${{ env.SNOWFLAKE_USER }}
          comment-on-pr: "true"

      - uses: Snowflake-Labs/snowflake_dcm_projects/actions/dcm-deploy@v1
        with:
          target: DCM_PROD_US
          project-path: ${{ env.DCM_PROJECT_PATH }}
          snowflake-user: ${{ env.SNOWFLAKE_USER }}
          test-expectations: "true"
          comment-on-pr: "true"
```

## Sample Workflows

The [`GitHub_workflows/`](../GitHub_workflows/) directory contains ready-to-use workflow files that demonstrate how these actions work together. You can copy them into your repository's `.github/workflows/` directory and customize them for your project. See the [Sample Workflows README](../GitHub_workflows/README.md) for setup instructions.
