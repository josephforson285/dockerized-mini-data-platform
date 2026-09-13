# Continuous Integration and Continuous Deployment

## Continuous Integration

CI is the practice of merging every developer's work into a shared mainline
frequently — at least daily — where each merge is verified automatically by a
build and a test suite. The point is not the server that runs the tests; it is
the short interval between changes. Integrating often keeps conflicts small and
makes the cause of a failure obvious, because only a little changed.

Three things make it CI rather than "we have a build job":

| Requirement | Why it matters |
| :-- | :-- |
| Changes merge to the mainline frequently | Long-lived branches defer integration pain rather than removing it |
| Every merge triggers an automated verification | A check people run manually is a check people skip |
| A failing mainline is fixed before anything else | If red is normal, the signal is worthless |

In this repository CI is the `lint`, `unit`, `dags` and `end-to-end` jobs in
[`.github/workflows/main.yml`](../.github/workflows/main.yml). They run on every
push and pull request, and `end-to-end` is gated behind the other three so a
syntax error never costs a ten-minute stack build.

## Continuous Delivery and Continuous Deployment

"CD" covers two different commitments, and the distinction is the substance of
the term.

**Continuous Delivery** means every build that passes the pipeline is *provably
releasable*. The artifact is built, versioned and verified in a
production-like environment; releasing it is a decision someone makes, not work
someone does. The gap between "green" and "live" is an approval, not an
engineering effort.

**Continuous Deployment** goes one step further: every build that passes is
released automatically, with no human gate. It requires the confidence that the
pipeline's verification is a sufficient substitute for human judgement.

| | Continuous Delivery | Continuous Deployment |
| :-- | :-- | :-- |
| Release is | a decision | automatic |
| Human gate | yes, deliberate | none |
| Needs | a verified, versioned artifact | that, plus enough test coverage to trust it unattended |

## Which this project does

Both, at different stages:

- **Continuous deployment to the test environment.** Every push to `main` that
  passes `end-to-end` publishes an image tagged `sha-<commit>` to GHCR, and
  `deploy-test` immediately pulls that exact image and runs the platform from
  it. No human is involved.
- **Continuous delivery to production, not deployment.** The same digest is
  ready to promote, but nothing promotes it. That is a deliberate stop: there is
  no production host behind this lab, and an automated deploy to nowhere would
  be theatre.

The artifact is tagged by commit SHA and never by `latest`. A mutable tag is
precisely how a pipeline reports green while the environment keeps running older
code — the deployed thing must be identifiable, or "we deployed the tested
build" is an assumption rather than a fact.
