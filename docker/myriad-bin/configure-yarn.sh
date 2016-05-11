#!/bin/bash
: '
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'

# Put in env defaults if they are missing
export YARN_USER=${YARN_USER:='yarn'}
export YARN_GROUP=${YARN_GROUP:='yarn'}
export HADOOP_GROUP=${HADOOP_GROUP:='hadoop'}
export YARN_UID=${USER_UID:='107'}
export YARN_GID=${YARN_GROUP_GID:='113'}
export HADOOP_GID=${HADOOP_GROUP_GID='112'}
export HADOOP_HOME=${HADOOP_HOME:='/usr/local/hadoop'}
export YARN_HOME=${YARN_HOME:=${HADOOP_HOME}}

# Add hduser user
echo "Creating $HADOOP_USER user.."
groupadd ${HADOOP_GROUP} -g ${HADOOP_GID}
groupadd ${YARN_GROUP} -g ${YARN_GID}
useradd ${YARN_USER} -g ${YARN_GROUP} -G ${HADOOP_GROUP} -u ${YARN_UID} -s /bin/bash -d /home/${YARN_USER}
mkdir /home/${YARN_USER}
chown -R $YARN_USER:$YARN_GROUP /home/${YARN_USER}
echo setting permissions on container-executor
chown root:${YARN_GROUP} ${YARN_HOME}/bin/container-executor
chmod 6050 ${YARN_HOME}/bin/container-executor
echo "end of configure-yarn.sh script"
