#!/bin/bash

set -e
shopt -s globstar

docker run -w /build -v $(pwd):/build prismagraphql/rust-build:latest cargo rustdoc

rm -rf deploy_docs
git clone --branch gh-pages "git@github.com:prisma/prisma-query.git" deploy_docs > /dev/null 2>&1
rm -rf deploy_docs/*
cd deploy_docs

git config user.name "Buildkite agent"
git config user.email "hello@prisma.io"

mv ../target/doc/* .

echo "<meta http-equiv=refresh content=0;url=prisma_query/index.html>" > index.html

git add -A .
git commit -m "Rebuild pages at ${BUILDKITE_COMMIT}"


$(git diff --exit-code --name-only HEAD HEAD~1)
if [ $? -eq 0 ]
then
    echo "Nothing to do"
else
    echo
    echo "Pushing docs..."
    git push --quiet origin gh-pages > /dev/null 2>&1
    echo
    echo "Docs published."
    echo
fi
