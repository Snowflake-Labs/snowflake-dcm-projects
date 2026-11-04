-- ### Secrets and environment variables
--
-- A value that must never be committed, such as an API key, cannot live in the
-- manifest's templating section. Declare its NAME in the manifest under
-- env_secrets, supply the value when you plan or deploy, and reference it here
-- with _snow.env_secret(). It renders as a reference rather than the value, so the
-- key never appears in this file, the rendered output, the changeset or the
-- deployment history. A value that is configurable but not sensitive, such as a
-- CI build number, is an env_var instead and renders as plain text.
--
-- The definition ships inside a Jinja comment, because a referenced env_secret
-- must be supplied on every plan and deploy. A SQL -- comment would not be enough:
-- Jinja would still evaluate the expressions inside it (see raw.sql).
--
-- Rotating: a change to the secret's value alone is not detected by plan today,
-- so change another property in the same deploy.
--
-- SECRETS STEP 2 of 2: delete the line that opens the Jinja comment below and the
-- line that closes it, after uncommenting the declarations in manifest.yml.

{#
DEFINE SECRET DCM_DEMO_1{{env_suffix}}.RAW.PARTNER_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = {{ _snow.env_secret("PARTNER_API_KEY") }}
    COMMENT = 'API key for the partner order feed, deployed by build {{ _snow.env_var("BUILD_NUMBER") }}';
#}
