#!/bin/sh
echo "Initializing localstack services"

echo "########### Creating level -1 bucket ###########"
awslocal s3api create-bucket --bucket ukceh-fdri-timeseries-level-m1 --region eu-west-2 --create-bucket-configuration LocationConstraint=eu-west-2