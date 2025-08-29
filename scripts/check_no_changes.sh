#!/usr/bin/env bash

# Check whether there are any changes to source files.
# If so, this means some code did not get committed.
git add -A

git diff --cached --name-only --quiet
if [ $? -eq 0 ]; then
  printf -- "No changes detected on working directory!\n\n"
else
  printf -- "There were changes in the working directory!\n"
  printf -- "This means some cached files should have been committed but were not\n"
  git diff --cached --name-only
  printf -- "More details\n"
  git diff --cached
  exit 1
fi
