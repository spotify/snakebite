#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

function print_color {
  local msg=${1?}
  local kind=${2:-GREEN}
  case "$kind" in
    RED)
      echo -e "$(tput setaf 1) $msg $(tput setaf 7)" >&2 ;;
    *)
      echo -e "$(tput setaf 2) $msg $(tput setaf 7)" ;;
  esac
}

function fatal_error {
  local msg=${1?"log message for fatal error not set"}
  print_color "$msg" RED
  exit 1
}

function usage {
  print_color "usage $0 <release-type> <committer-fullname> <committer-email> [change-log-desc]" RED
  echo
  print_color "\trelease types: revision, minor, major" RED
  print_color "\tchange-log-desc is optional, default - 'Bump version'" RED
  exit 1
}

release_type=${1?$(usage)"relese not set"}
committer_fullname=${2?$(usage)"committer name not set"}
committer_email=${3?$(usage)"committer email not set"}
change_log_desc=${4:-"Bump version"}

if ! which git &>/dev/null; then
  fatal_error "Git not available - install git!"
fi

if ! which python &>/dev/null; then
  fatal_error "python no available - install python!"
fi

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VERSION_FILE="$DIR_OF_SCRIPT/../snakebite/version.py"
CHANGELOG_FILE="$DIR_OF_SCRIPT/../debian/changelog"

if ! git diff-index --quiet HEAD --; then
  fatal_error "You have dirty work tree - clean it up before release"
fi

version=$(sed -n "s/.*VERSION = \"\([^']*\)\".*/\1/p" $VERSION_FILE)

version_part=(${version//./ })
major=${version_part[0]}
minor=${version_part[1]}
revision=${version_part[2]}

print_color "Current git HEAD is $(git rev-parse HEAD)"
print_color "Current snakebite full version is $version"
print_color "Current snakebite major version is $major"
print_color "Current snakebite minor version is $minor"
print_color "Current snakebite revision version is $revision"

print_color "$release_type release requested"

case "$release_type" in
  revision)
    revision=$(($revision + 1))
    ;;
  minor)
    minor=$(($minor + 1))
    revision=0
    ;;
  major)
    major=$(($major + 1))
    minor=0
    revision=0
    ;;
  *)
    fatal_error "Error: wrong release type - '$release_type'"
    ;;
esac

old_version=$version
version="${major}.${minor}.${revision}"
print_color "Future snakebite full version will be $version"

temp_changelog=$(mktemp tmp.XXXX)

trap "rm -f $temp_changelog; rm -f ${VERSION_FILE}.release_bak; \
fatal_error \"Something went wrong - please check your local changes \
- revert if needed\"" EXIT

sed -i.release_bak "s/$old_version/$version/g" $VERSION_FILE
rm ${VERSION_FILE}.release_bak

print_color "Version file updated:"
git --no-pager diff --no-prefix $VERSION_FILE

rfc_date=$(echo "from email.Utils import formatdate
print formatdate()" | python -)

echo -e "snakebite ($version) unstable; urgency=low

  $change_log_desc

 -- $committer_fullname <$committer_email>  $rfc_date
" | cat - $CHANGELOG_FILE > $temp_changelog && mv $temp_changelog $CHANGELOG_FILE

print_color "Debian changelog file updated:"
git --no-pager diff --no-prefix $CHANGELOG_FILE

print_color "Commit changes in $VERSION_FILE and $CHANGELOG_FILE"
git add $VERSION_FILE $CHANGELOG_FILE
git commit -m "Release version $version"

print_color "Add tag $version"
git tag $version

trap - EXIT

print_color "Release prepared to go live - check changes, push code and distribution for release $version"
