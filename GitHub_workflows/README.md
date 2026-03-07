# DCM Project - GitHub Actions Workflows

These workflows automate the full lifecycle of a [Snowflake DCM (Database Change Management) Project](https://docs.snowflake.com/en/LIMITEDACCESS/dcm-projects/snowflake-dcm-projects) using GitHub Actions. They cover connection testing, PR validation, deployment with safety gates, and data quality testing.

## Prerequisites

- A Snowflake account that is enrolled in the preview of Snowflake DCM Projects
- A DCM project with a valid `manifest.yml` containing at least one target
- A Snowflake service user with credentials for each target account (see [Authentication](#4-set-repository-secrets) below)

## How the Workflows Work Together

| # | Workflow | Trigger | Purpose |
|---|---------|---------|---------|
| 1 | **Test Connections** | Manual | Validates connectivity and role configuration for all manifest targets |
| 2 | **Test PR to main** | PR to `main` / Manual | Runs `snow dcm plan` against STAGE and PROD in parallel, posts results as a PR comment |
| 3 | **Deploy to STAGE & PROD** | Push to `main` / Manual | Full sequential pipeline: Plan → Data Drop Detection → Deploy → Post Scripts → Refresh DTs → Test Expectations (STAGE first, then PROD) |
| 4 | **Test STAGE Expectations** | Manual | Refreshes dynamic tables and runs data quality expectations on STAGE |

**Typical flow:** Run **Workflow 1** once to validate your setup. Then use the PR-based flow: open a PR (triggers **Workflow 2** for plan preview), merge to main (triggers **Workflow 3** for deployment). Use **Workflow 4** for ad-hoc data quality testing.

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

Go to **Settings → Environments** in your GitHub repository and create one environment per manifest target.

For the default configuration, create:

| Environment | Maps to manifest target |
|------------|------------------------|
| `DCM_STAGE` | `targets.DCM_STAGE` |
| `DCM_PROD_US` | `targets.DCM_PROD_US` |

> **Tip:** You can add environment protection rules (e.g. required reviewers) on `DCM_PROD_US` to gate production deployments with manual approval.

### 3. Set Repository Variables

Go to **Settings → Secrets and variables → Actions → Variables** and create:

| Variable | Value | Example |
|----------|-------|---------|
| `DCM_PROJECT_PATH` | Relative path from repo root to your DCM project directory (must end with `/`) | `Quickstarts/DCM_Project_Quickstart_1/` |
| `SNOWFLAKE_USER` | The Snowflake username used for deployments | `GITHUB_ACTIONS_SERVICE_USER` |

These are **repository-level** variables, shared across all environments. If you need different users per environment, move `SNOWFLAKE_USER` to an environment-level variable instead.

### 4. Set Repository Secrets

Go to **Settings → Secrets and variables → Actions → Secrets**.

The workflows authenticate using environment variables that the Snowflake CLI picks up automatically. Out of the box, they use **password-based authentication** (programmatic access token). You can also use **key-pair authentication** — see both options below.

#### Option A: Password / Programmatic Access Token (default)

| Secret | Value |
|--------|-------|
| `DEPLOYER_PAT` | The programmatic access token (password) for `SNOWFLAKE_USER` |

This is what the workflows use as shipped. No other changes needed.

#### Option B: Key-Pair Authentication

If you prefer key-pair auth, create these secrets instead:

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_PRIVATE_KEY_RAW` | The full PEM-encoded private key content (not a file path) |

Then update the `env` block in each workflow file — replace:

```yaml
env:
  SNOWFLAKE_PASSWORD: ${{ secrets.DEPLOYER_PAT }}
```

with:

```yaml
env:
  SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
  SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
```

> **Note:** `SNOWFLAKE_PRIVATE_KEY_RAW` is recommended over `SNOWFLAKE_PRIVATE_KEY_FILE` for CI/CD because GitHub Actions runners don't have persistent local file storage.

If your STAGE and PROD accounts use different credentials, create secrets as **environment-level secrets** on each environment instead of repository-level secrets.

### 5. Set Workflow Permissions

Go to **Settings → Actions → General → Workflow permissions** and ensure:

- **Read and write permissions** is selected (required for Workflows 2 and 3 to post PR comments)

### 6. Configure Path Filters (if needed)

Workflows 2 and 3 filter on file changes under `Quickstarts/**`. Update the `paths` filter in both workflow files to match your project structure:

```yaml
on:
  push:
    branches: ["main"]
    paths:
      - 'your/project/path/**'   # Change this to match your DCM project location
```

## Environment Variable Flow

The workflows use a consistent pattern to authenticate with Snowflake. Understanding this flow helps with debugging:

1. **`DCM_PROJECT_PATH`** (repo variable) → locates `manifest.yml` in the repository
2. **`manifest.yml`** is parsed with `yq` to extract `account_identifier`, `project_owner`, and `project_name` for each target
3. These values are passed as job outputs and set as environment variables:
   - `SNOWFLAKE_ACCOUNT` ← from `account_identifier`
   - `SNOWFLAKE_ROLE` ← from `project_owner`
4. **`SNOWFLAKE_USER`** (repo variable) and the authentication secret (`SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_RAW`) complete the connection
5. The Snowflake CLI picks up all `SNOWFLAKE_*` environment variables automatically — no `connections.toml` file is needed

## Customizing for Your Project

### Different target names

If your manifest uses different target names (e.g. `STAGING`, `PRODUCTION`), you need to:

1. Create GitHub environments with matching names
2. Update the hardcoded target references in Workflows 2–4 (search for `DCM_STAGE` and `DCM_PROD_US`)

Workflow 1 is fully dynamic — it reads all targets from the manifest automatically.

### Single-target deployment

To deploy to only one environment, remove the PROD jobs from Workflows 2 and 3, and adjust the `needs` dependencies accordingly.

### Post-hook scripts

Workflow 3 executes `hooks/post_hook.sql` after each deployment using Jinja templating. The `env_suffix` variable is passed to differentiate environments (`STG` for STAGE, `PROD` for production). If you don't use post-hooks, those jobs will simply report "No post-hook files found."

### Data drop detection

Workflow 3 includes a safety gate that blocks deployment if the plan contains DROP operations on databases, schemas, tables, or stages. This protects against accidental data loss. If a DROP is intentional, you'll need to temporarily adjust the detection logic or manually approve the workflow run.
