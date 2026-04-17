# DCM Project - Sample GitHub Actions Workflows

These are **sample workflows** that demonstrate how to use the [reusable DCM GitHub Actions](../actions/README.md) to automate the full lifecycle of a [Snowflake DCM (Database Change Management) Project](https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-overview).

You can copy these workflows into your repository's `.github/workflows/` directory and customize them for your project. You can also build your own workflows using the individual actions directly — see the [Actions README](../actions/README.md) for full documentation of each action.

## Prerequisites

- A Snowflake account that is enrolled in the public preview of Snowflake DCM Projects
- A DCM project with a valid `manifest.yml` containing at least one target
- A Snowflake service user configured for authentication (see [Authentication](#4-configure-authentication) below)

## How the Workflows Work Together

Each workflow is composed from the reusable actions in [`actions/`](../actions/). The actions handle Snowflake CLI setup, OIDC authentication, manifest parsing, and all DCM commands internally.

| # | Workflow | Trigger | Purpose |
|---|---------|---------|---------|
| 1 | **Test Connections** | Manual | Validates connectivity and role configuration for all manifest targets |
| 2 | **Test PR to main** | PR to `main` | Runs `snow dcm plan` against PROD and optionally posts results as a PR comment |
| 3 | **Deploy to PROD** | Push to `main` | Plan and deploy to PROD with optional drop detection, post-scripts, Dynamic Tables refresh, and data expectation testing |
| 4 | **Deploy to STAGE then PROD** | Push to `main` | Full sequential pipeline: STAGE first (plan, deploy, test), then PROD (plan, deploy, test) |

**Typical flow:** Run **Workflow 1** once to validate your setup. Then use the PR-based flow: open a PR (triggers **Workflow 2** for plan preview), merge to main (triggers **Workflow 3** or **4** for deployment). Choose Workflow 3 if you deploy to PROD only, or Workflow 4 if you want a STAGE-then-PROD pipeline.

## Setup

### 1. Configure your `manifest.yml`

Your manifest defines the deployment targets. Each target name becomes a GitHub environment name. Example:

```yaml
manifest_version: 2
type: DCM_PROJECT

targets:
  DCM_STAGE:
    account_identifier: YOUR_STAGE_ACCOUNT   # e.g. MYORG-STAGE_ACCOUNT
    project_name: MY_DB.MY_SCHEMA.MY_PROJECT_STG
    project_owner: MY_STAGE_DEPLOYER_ROLE
    templating_config: STAGE

  DCM_PROD_US:
    account_identifier: YOUR_PROD_ACCOUNT    # e.g. MYORG-PROD_ACCOUNT
    project_name: MY_DB.MY_SCHEMA.MY_PROJECT_PROD
    project_owner: MY_PROD_DEPLOYER_ROLE
    templating_config: PROD
```

The target names (`DCM_STAGE`, `DCM_PROD_US`) must exactly match the GitHub environment names you create in the next step.

### 2. Create GitHub Environments

Go to **Settings > Environments** in your GitHub repository and create one environment per manifest target.

For the default configuration, create:

| Environment | Maps to manifest target |
|------------|------------------------|
| `DCM_STAGE` | `targets.DCM_STAGE` |
| `DCM_PROD_US` | `targets.DCM_PROD_US` |

> **Tip:** You can add environment protection rules (e.g. required reviewers) on `DCM_PROD_US` to gate production deployments with manual approval.

### 3. Set Workflow Variables

The sample workflows define the DCM project path and Snowflake username in the workflow-level `env:` block. Update these values in each workflow file you use:

```yaml
env:
  DCM_PROJECT_PATH: your/project/path/       # relative path from repo root (must end with /)
  SNOWFLAKE_USER: GITHUB_ACTIONS_SERVICE_USER # Snowflake username for deployments
```

> **Tip:** You can alternatively store these as **repository variables** (Settings > Secrets and variables > Actions > Variables) and reference them with `${{ vars.SNOWFLAKE_USER }}` / `${{ vars.DCM_PROJECT_PATH }}` instead. If you need different users per environment, use environment-level variables.

### 4. Configure Authentication

The actions authenticate using the [Snowflake CLI GitHub Action](https://github.com/snowflakedb/snowflake-cli-action) (`snowflakedb/snowflake-cli-action@v2.0.1`). **OIDC is the recommended and default approach** — the actions call the CLI action with `use-oidc: true` internally.

#### OIDC (recommended — used by default)

With OIDC, GitHub's identity tokens authenticate directly with Snowflake. No passwords or private keys need to be stored as secrets.

To use OIDC:

1. Create a Snowflake service user and configure a security integration that trusts GitHub's OIDC provider
2. Grant your workflow `id-token: write` permission (already set in the sample workflows)
3. Set `SNOWFLAKE_USER` in the workflow `env` block (step 3 above)

No secrets are required — the GitHub environment's OIDC token handles authentication automatically.

The actions handle authentication internally — see the [Actions README Authentication section](../actions/README.md#authentication) for full setup instructions (OIDC, PAT, and key-pair options).

#### Alternative: PAT / Password

If you cannot use OIDC, you can authenticate with a programmatic access token. Create a repository secret:

| Secret | Value |
|--------|-------|
| `DEPLOYER_PAT` | The programmatic access token (password) for `SNOWFLAKE_USER` |

Then add an `env` block to each workflow (or to individual jobs):

```yaml
env:
  SNOWFLAKE_PASSWORD: ${{ secrets.DEPLOYER_PAT }}
```

#### Alternative: Key-Pair Authentication

Create a repository secret:

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_PRIVATE_KEY_RAW` | The full PEM-encoded private key content (not a file path) |

Then add an `env` block to each workflow:

```yaml
env:
  SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
  SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
```

> **Note:** If your STAGE and PROD accounts use different credentials, create secrets as **environment-level secrets** on each environment instead of repository-level secrets.

### 5. Set Workflow Permissions

Go to **Settings > Actions > General > Workflow permissions** and ensure:

- **Read and write permissions** is selected (required for Workflows 2, 3, and 4 to post PR comments)

### 6. Configure Path Filters (if needed)

Workflows 2, 3, and 4 filter on file changes under `Quickstarts/**`. Update the `paths` filter in each workflow file to match your project structure:

```yaml
on:
  push:
    branches: ["main"]
    paths:
      - 'your/project/path/**'   # Change this to match your DCM project location
```

## Environment Variable Flow

The actions use a consistent pattern to authenticate with Snowflake. Understanding this flow helps with debugging:

1. **`DCM_PROJECT_PATH`** (workflow `env` value) locates `manifest.yml` in the repository
2. **`manifest.yml`** is parsed to extract `account_identifier`, `project_owner`, and `project_name` for each target
3. These values are set as environment variables inside the action:
   - `SNOWFLAKE_ACCOUNT` -- from `account_identifier`
   - `SNOWFLAKE_ROLE` -- from `project_owner`
4. **`SNOWFLAKE_USER`** (workflow `env` value) is passed to the action via the `snowflake-user` input
5. **Authentication** is handled by the Snowflake CLI action. With OIDC (default), the GitHub environment's identity token is used. With PAT or key-pair, the corresponding `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_RAW` environment variable is picked up from your workflow's `env` block.
6. The Snowflake CLI picks up all `SNOWFLAKE_*` environment variables automatically — no `connections.toml` file is needed

## Customizing for Your Project

### Different target names

If your manifest uses different target names (e.g. `STAGING`, `PRODUCTION`), you need to:

1. Create GitHub environments with matching names
2. Update the hardcoded target references in Workflows 2-4 (search for `DCM_STAGE` and `DCM_PROD_US`)

Workflow 1 is fully dynamic — it reads all targets from the manifest automatically.

### Single-target deployment

To deploy to only one environment, use Workflow 3 as a starting point (it targets PROD only). Adjust the target name and environment as needed.

### Post-hook scripts

Workflows 3 and 4 execute SQL files from a `post-scripts-path` directory after each deployment using Jinja templating. Manifest templating variables are passed automatically. If you don't use post-scripts, those steps will simply report "No .sql files found."

### Data drop detection

Workflows 3 and 4 include a safety gate that blocks deployment if the plan contains DROP operations on databases, schemas, tables, or stages. This protects against accidental data loss. Set `allow-drops: "true"` on the `dcm-deploy` action to bypass this check when a DROP is intentional.
