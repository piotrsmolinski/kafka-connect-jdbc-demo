#!/usr/bin/env bash
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

. /etc/confluent/docker/bash-config

. /etc/confluent/docker/mesos-setup.sh
. /etc/confluent/docker/apply-mesos-overrides

# echo "===> User"
# id

echo "===> Configuring ..."
/etc/confluent/docker/configure

# Modification: in the integration test we do not need redundant checks
# echo "===> Running preflight checks ... "
# /etc/confluent/docker/ensure

echo "===> Launching ... "
exec /etc/confluent/docker/launch