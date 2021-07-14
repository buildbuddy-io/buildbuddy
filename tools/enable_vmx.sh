#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

# adapted from https://issuetracker.google.com/issues/110507927#comment22
# this script clones a GCE instance template and enables nested virtualization
# on it.
# ex template url: "https://www.googleapis.com/compute/v1/projects/flame-build/global/instanceTemplates/gke-dev-nv8eh-c2-pool-fad3284d"
# template url as $1

# GET auth token to be used in curl/rest api
AUTH_TOKEN=$(gcloud auth print-access-token)

function isGoogResExists {
  PARAMS=(
    -s
    -I
    -o /dev/null
    -w "%{http_code}"
    -H "Authorization: Bearer ${AUTH_TOKEN}"
    -H "Content-Type: application/json"
  )
  status_code=$(curl "${PARAMS[@]}" $1);
  if [ $status_code -eq 404 ]; then
    return 1
  elif [ $status_code -eq 200 ]; then
    return 0
  fi
}

function makeGoogGetReq {
  PARAMS=(
    -s
    -H "Authorization: Bearer ${AUTH_TOKEN}"
    -H "Content-Type: application/json"
  )
  curl "${PARAMS[@]}" $1;
}

function makeGoogPostReq {
  PARAMS=(
    -s
    -XPOST
    -H "Authorization: Bearer ${AUTH_TOKEN}"
    -H "Content-Type: application/json"
  )
  curl "${PARAMS[@]}" $1 --data "$2";
}


it_url=$1
BASE_IT_URL="${it_url%/instanceTemplates*}/instanceTemplates"
it_json=$(makeGoogGetReq $it_url);

# Duplicate instance template and enable nested virtualization.
it_name=$(echo $it_json | jq -r '.name')
if [[ "$it_name" != *-vmx ]]; then
    it_name="${it_name}-vmx";
fi
new_it_url="${BASE_IT_URL}/${it_name}"
if isGoogResExists "$new_it_url"; then
    echo "Instance Template exists.."
else
    echo "Creating new InstanceTemplate.."
    OPS=(
	'.name = $it_name'
	'.properties.advancedMachineFeatures.enableNestedVirtualization = true'
    )
    printf -v _OPS " | %s" "${OPS[@]}"
    ARGS=(
	--arg it_name "$it_name"
    )
    new_it_json=$(echo "$it_json" | jq -r ". $_OPS" "${ARGS[@]}");
    makeGoogPostReq $BASE_IT_URL "$new_it_json"  
fi
