#!/bin/bash

authToken=$1
collectionId=$2
githubUsername=$3
githubToken=$4

# TODO(siggisim): Support pagination when we have more than 100 docs
listResponse=`curl https://api.webflow.com/collections/$collectionId/items \
    -H "Authorization: Bearer $authToken" \
    -H 'accept-version: 1.0.0'`

# Fetch existing docs and shove them into a "map"
items=$(echo $listResponse | jq -c '.items[]')
while IFS= read -r line
do
  slug=$(echo $line | jq -j '.slug, "\n"')
  key=$(echo $slug | tr -cd '[:alnum:]')
  id=$(echo $line | jq -j '._id, "\n"')
  echo "✓ Found ${slug} with id ${id}"
  declare "itemMap_$key=$id"
done <<< "$items"

# Loop over files in the docs directory
for fileName in ./docs/*
do

# Filter out any non-markdown files
if [ ${fileName: -3} != ".md" ]; then
continue
fi

fileContents=`cat $fileName`
slug=${fileName%.*}
slug=${slug:7}

if [[ ${fileContents:0:4} != "<!--" ]]
then
echo "Skipping $slug, no comment found"
continue
fi

# Convert markdown to HTML using github api
htmlContents=`curl -u $githubUsername:$githubToken -H "Content-Type: text/plain" --data-binary @$fileName https://api.github.com/markdown/raw | jq -aRs`

# TODO(siggisim): Make these rewrites less fragile
# Rewrite .md links
htmlContents=${htmlContents//.md/}
# Remove user-content id tags
htmlContents=${htmlContents//user-content-/}

# Parse metadata from comment at the beginning of the file
metadata=`echo $fileContents | awk -F '-->' '{print $1}'`
metadata=${metadata:6}
metadata="{\"slug\": \"$slug\", \"_archived\": false, \"_draft\": false, \"article-body\": $htmlContents, $metadata"

# Get the item id if it exists
key=$(echo $slug | tr -cd '[:alnum:]')
mapKey="itemMap_$key"
itemId=$(printf '%s' "${!mapKey}")

method="POST"
if [[ $itemId ]]; then
method="PATCH"
itemId="/$itemId"
echo "UPDATING $slug with item id $itemId..."
else
echo "CREATING $slug..."
fi

curl -X $method "https://api.webflow.com/collections/$collectionId/items$itemId?live=true" \
  -H "Authorization: Bearer $authToken" \
  -H 'accept-version: 1.0.0' \
  -H "Content-Type: application/json" \
  --data-binary $"{
      \"fields\": $metadata
    }" >> /dev/null
# Make sure we don't surpass the 1 qps rate limit
sleep 1
done