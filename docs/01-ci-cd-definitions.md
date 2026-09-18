# Continuous Integration and Continuous Deployment

## Continuous Integration

Continuous Integration (CI) means integrating changes into the shared codebase frequently and validating them automatically.

Effective CI relies on three principles:

| Principle                  | Purpose                                        |
| :------------------------- | :--------------------------------------------- |
| Frequent integration       | Keeps conflicts small and easier to resolve    |
| Automated checks           | Ensures every change is consistently validated |
| Fix failing builds quickly | Keeps the main branch reliable                 |

In this project, CI is implemented in [`.github/workflows/main.yml`](../.github/workflows/main.yml) through:

* `lint`
* `unit`
* `dags`
* `end-to-end`

These checks run on pushes and pull requests. The end-to-end stage runs only after the faster checks succeed.

## Continuous Delivery vs Continuous Deployment

**Continuous Delivery** means every successful build is tested, versioned, and ready for release, but production deployment still requires a deliberate approval.

**Continuous Deployment** automatically releases every build that successfully passes the pipeline.

|             | Continuous Delivery          | Continuous Deployment                           |
| :---------- | :--------------------------- | :---------------------------------------------- |
| Release     | Manually approved            | Automatic                                       |
| Human gate  | Yes                          | No                                              |
| Requirement | Releasable verified artifact | Verified artifact trusted for automatic release |

## What This Project Implements

The project uses both approaches at different stages.

**Test environment — Continuous Deployment**

After a successful pipeline on `main`:

1. The image is built and tagged with `sha-<commit>`.
2. It is pushed to GHCR.
3. `deploy-test` automatically pulls and smoke-tests that exact image.

No manual approval is required.
 