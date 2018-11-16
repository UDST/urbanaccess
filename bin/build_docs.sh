#! /usr/bin/env bash

# Copied from github.com/sympy/sympy
#
# This file automatically deploys changes to http://udst.github.io/urbanaccess/.
# This will only happen when building a non-pull request build on the master
# branch of Pandana.
# It requires an access token which should be present in .travis.yml file.
#
# Following is the procedure to get the access token:
#
# $ curl -X POST -u <github_username> -H "Content-Type: application/json" -d\
# "{\"scopes\":[\"public_repo\"],\"note\":\"token for pushing from travis\"}"\
# https://api.github.com/authorizations
#
# It'll give you a JSON response having a key called "token".
#
# $ gem install travis
# $ travis encrypt -r sympy/sympy GH_TOKEN=<token> env.global
#
# This will give you an access token("secure"). This helps in creating an
# environment variable named GH_TOKEN while building.
#
# Add this secure code to .travis.yml as described here http://docs.travis-ci.com/user/encryption-keys/

# Exit on error
set -e

ACTUAL_TRAVIS_JOB_NUMBER=`echo $TRAVIS_JOB_NUMBER| cut -d'.' -f 2`

echo "Starting docs build..."
echo $RAVIS_REPO_SLUG
echo $TRAVIS_BRANCH
echo $TRAVIS_PULL_REQUEST
echo $ACTUAL_TRAVIS_JOB_NUMBER
echo $GH_TOKEN
echo $TRAVIS_BUILD_NUMBER

#if [ "$TRAVIS_REPO_SLUG" == "UDST/urbanaccess" ] && \
#        [ "$TRAVIS_BRANCH" == "master" ] && \
#        [ "$TRAVIS_PULL_REQUEST" == "false" ] && \
#        [ "$ACTUAL_TRAVIS_JOB_NUMBER" == "1" ]; then

        echo "Installing dependencies"
        conda install --yes sphinx numpydoc
        pip install sphinx_rtd_theme

        echo "Building docs"
        cd docs
        make clean
        make html

#        cd ../../
#        echo "Setting git attributes"
#        git config --global user.email "fernandez@urbansim.com"
#        git config --global user.name "udst-documentator"

#        echo "Cloning repository"
#        git clone --quiet --single-branch --branch=gh-pages https://${GH_TOKEN}@github.com/udst/urbanaccess.git  gh-pages > /dev/null 2>&1

#        cd gh-pages
#        rm -rf *
#        cp -R ../urbanaccess/docs/build/html/* ./
#        git add -A .
#
#        git commit -am "Update dev docs after building $TRAVIS_BUILD_NUMBER"
#        echo "Pushing commit"
#        git push -fq origin gh-pages # > /dev/null 2>&1
#fi
