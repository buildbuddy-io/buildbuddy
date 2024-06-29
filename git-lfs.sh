#/bin/bash -x

cd $BUILD_WORKSPACE_DIRECTORY

ls -al .git/hooks/

sudo apt update && sudo apt install -y git-lfs

git lfs install

ls -al .git/hooks/
