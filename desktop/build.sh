#!/bin/bash

set -e

echo "Building BuildBuddy Desktop App..."

cd BuildBuddyApp

xcodebuild -project BuildBuddyApp.xcodeproj \
           -scheme BuildBuddyApp \
           -configuration Release \
           -derivedDataPath build \
           clean build

echo "Build complete!"
echo "App location: BuildBuddyApp/build/Build/Products/Release/BuildBuddyApp.app"

read -p "Do you want to open the app? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    open build/Build/Products/Release/BuildBuddyApp.app
fi