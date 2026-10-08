# Lab 2: S3 - Answers

## Why are the credentials stored in a Secret and not in the ConfigMap?

A ConfigMap is used for configuration that is not confidential, like the S3 server address or the bucket name. Anyone
with access to the namespace can read it in plain text. The credentials give access to the data, so they belong in a
Secret, which is designed for sensitive information. Access to it can be restricted separately, its content is not
displayed by default, and it can be encrypted inside the cluster.

## What happens if the Job is executed again tomorrow?

It would fail. The credentials provided by Onyxia are temporary (mine expired the following night), so tomorrow the
Job would get an `ExpiredToken` error. On a real platform, we would not copy credentials into a Secret by hand.
Instead, the Job would get credentials that are renewed automatically, for example by linking its Kubernetes service
account to a role that has access to the storage, or by using a secrets manager like Vault.

## How would you turn this Job into a daily ingestion?

We just need to replace the `Job` with a `CronJob`. It is a Kubernetes object that automatically starts a Job on a
schedule, for example `schedule: "0 2 * * *"` to run it every day at 2 a.m. In a real situation, the data would not
come from a ConfigMap but directly from the source system.
