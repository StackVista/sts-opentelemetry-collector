#!/usr/bin/env bash

# Check whether there are any changes to source files.
# If so, this means some code did not get committed.
git add -A
if [[ $( git diff --cached --name-only --quiet ) ]]; then
  printf -- "There were changes in the working directory!\n"
  printf -- "This means some cached files should have been committed but were not\n"
  git diff --cached --name-only
  exit 1
else
  printf -- "No changes detected on working directory!\n\n"
fi
