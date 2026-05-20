FROM docker.io/apache/fluss:0.9.1-incubating

# Workshop-pinned Fluss server image. The base image ships both the
# CoordinatorServer and TabletServer modes — selected at runtime via the
# `command:` field in podman-compose.yml.
#
# No code changes here; this Dockerfile exists so the workshop's
# podman-compose `build:` blocks can pin the image tag without
# pulling-on-every-startup. If you bump Fluss versions, change the FROM line
# *and* the FLUSS_VERSION property in the root pom.xml.

# (No additional steps required — Fluss bundles its own server start scripts.)
