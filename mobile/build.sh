#!/bin/bash

set -e

echo "Building BuildBuddy Mobile App for iOS..."

cd BuildBuddyMobile

# Build for iOS Simulator
xcodebuild -project BuildBuddyMobile.xcodeproj \
           -scheme BuildBuddyMobile \
           -configuration Release \
           -sdk iphonesimulator \
           -derivedDataPath build \
           clean build

echo "Build complete!"
echo "App location: BuildBuddyMobile/build/Build/Products/Release-iphonesimulator/BuildBuddyMobile.app"

read -p "Do you want to open the app in the iOS Simulator? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Open iOS Simulator
    open -a Simulator
    
    # Install and run the app
    xcrun simctl install booted build/Build/Products/Release-iphonesimulator/BuildBuddyMobile.app
    xcrun simctl launch booted com.buildbuddy.BuildBuddyMobile
fi