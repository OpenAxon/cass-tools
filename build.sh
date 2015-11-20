#!/bin/sh -u

if [ $BRANCH_NAME == "master" ];
then
  export BUILD_QUALIFIER=""
else
  export BUILD_QUALIFIER="-$BRANCH_NAME-SNAPSHOT"
fi

./activator compile outputVersion package publish universal:packageBin -Dpackaging.buildQualifier="$BUILD_QUALIFIER" -Dpackaging.buildNumber="$BUILD_NUMBER"
./activator coverage test
./activator coverageReport