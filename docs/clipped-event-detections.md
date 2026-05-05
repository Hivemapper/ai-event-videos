# Clipped Event Processing

Clipped event IDs such as `69f77fe3bfa5ee05de0b100c-1Secto20Sec` are local edited events. They do not exist in Bee Maps under the suffixed ID, so AWS detector and production workers need the local clip package before claiming the run.

When `/api/videos/[videoId]/runs` queues an AWS-backed run, it now checks for local clipped-event assets and syncs them before inserting the detection run:

- `data/metadata/<base-id>.json`
- `data/metadata/<clipped-id>.json`
- `public/videos/<clipped-id>.mp4`
- `data/edited-events/<base-id>/`

The sync target hosts come from `DETECTION_AWS_ASSET_HOSTS`, `DETECTION_AWS_HOSTS`, or `DETECTION_AWS_HOST`. If none are set, the route tries to discover running EC2 instances whose Name tag matches `vru` or `detect`.

Useful environment variables:

```bash
DETECTION_RUNNER_MODE=aws-queue
DETECTION_AWS_ASSET_HOSTS=34.220.84.9,52.33.236.250,52.36.246.33
DETECTION_AWS_SSH_USER=ec2-user
DETECTION_AWS_SSH_KEY=~/Downloads/vru.pem
DETECTION_AWS_PROJECT_DIR=/home/ec2-user/ai-event-videos
```

Set `DETECTION_AWS_SYNC_CLIPPED_ASSETS=0` to disable the automatic sync.

Manual production enqueueing uses the same asset package for clipped IDs before it writes or requeues the `production_runs` row. Production sync targets come from `PRODUCTION_AWS_ASSET_HOSTS`, `PRODUCTION_AWS_HOSTS`, or `PRODUCTION_AWS_HOST`. If those are unset, the route tries to discover running EC2 instances tagged with `fleet=prod-pipeline-fleet`.

Useful production variables:

```bash
PRODUCTION_AWS_ASSET_HOSTS=54.149.110.164
PRODUCTION_AWS_SSH_USER=ec2-user
PRODUCTION_AWS_SSH_KEY=~/Downloads/vru.pem
PRODUCTION_AWS_PROJECT_DIR=/home/ec2-user/ai-event-videos
```

Set `PRODUCTION_AWS_SYNC_CLIPPED_ASSETS=0` to disable the production-side automatic sync.

The persistent detector only deletes transient files under `data/pipeline-video-cache`. Local clipped MP4s under `public/videos/` and `data/edited-events/` must remain in place after detection so a later retry or production run can reuse the same clip package.
