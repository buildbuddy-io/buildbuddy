#/bin/bash -x

cd $BUILD_WORKSPACE_DIRECTORY

sudo apt update && sudo apt install -y git-lfs

ls -al .git/hooks/

git lfs install

ls -al .git/hooks/

