#!/usr/bin/env bash

mysql -uroot -pmysqlPass <<EOF
create database test;
create database test_active_schema;
create database pgtid_meta;
EOF
