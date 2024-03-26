#!/bin/bash
set -e
set -x

INSTANCE_NAME="gke-dev-nv8eh-executor-4623ae0f-grp-drwr"
NEW_INSTANCE_NAME="rcu-debug-dev"

INSTANCE_ZONE="$(gcloud compute instances list --filter="name=($INSTANCE_NAME)" --format="value(zone)")"

if [ -z "$INSTANCE_ZONE" ]; then
  echo "Instance zone not found for $INSTANCE_NAME. Please check if the instance exists and you have the correct permissions."
  exit 1
fi

gcloud compute instances stop "$INSTANCE_NAME" --zone="$INSTANCE_ZONE"

SOURCE_DISK_NAME="$(gcloud compute instances describe "$INSTANCE_NAME" --zone="$INSTANCE_ZONE" --format='get(disks[0].source)' | awk -F'/' '{print $NF}')"
if [ -z "$SOURCE_DISK_NAME" ]; then
  echo "Source disk name not found for $INSTANCE_NAME. Please check the instance details."
  exit 1
fi

gcloud compute images create "${NEW_INSTANCE_NAME}-image" --source-disk="$SOURCE_DISK_NAME" --source-disk-zone="$INSTANCE_ZONE"
gcloud compute instances create "$NEW_INSTANCE_NAME" --zone="$INSTANCE_ZONE" --image="${NEW_INSTANCE_NAME}-image"

if [ $? -eq 0 ]; then
  echo "New instance created successfully."
else
  echo "Failed to create a new instance."
  exit 1
fi
