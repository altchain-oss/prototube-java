#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function check_buildwarning() {
	result=$(mvn compile test-compile install)
	echo $result
	if [ $? -ne 0 ]; then
		exit $?
	fi
	warnings=$(echo $result|grep -v "generated-sources"|grep -c "[WARNING]")
	if [ $? -eq 0 ]; then
		echo "There are ${warnings} warnings in compilation."
		exit 1
	fi
}

mvn clean \
&& mvn enforcer:enforce --batch-mode --fail-at-end \
&& mvn checkstyle:check --batch-mode --fail-at-end \
&& mvn apache-rat:check \
&& check_buildwarning \
&& mvn spotbugs:check --batch-mode --fail-at-end \
&& mvn verify --batch-mode --fail-at-end
