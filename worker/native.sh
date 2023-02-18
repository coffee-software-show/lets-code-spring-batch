#!/usr/bin/env bash
./gradlew clean
./gradlew nativeCompile
./build/native/nativeCompile/worker
