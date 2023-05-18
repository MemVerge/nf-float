#!/bin/bash
#
# Copyright (C) 2023 MemVerge Inc.


# set output
echo "set the github actions output"

zip_path=$(find . -name "nf-float-*.zip")
echo "* zip_path=${zip_path}"

zip_filename=$(basename "${zip_path}")
echo "* zip_filename=${zip_filename}"

echo "::set-output name=zip_filename::${zip_filename}"
echo "::set-output name=zip_path::${zip_path}"
