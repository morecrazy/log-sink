# This dockerfile uses the go image
# VERSION 2 - EDITION 1
# Author: zhanghan
# Command format: Instruction [arguments / command] ..

# Base image to use, this must be set as the first line
FROM dockerhub.codoon.com/centos

# Maintainer: docker_user <docker_user at email.com> (@docker_user)
MAINTAINER zhanghan zhanghan@codoon.com

# Set LABEL
LABEL name="log-sink" author="zhanghan" branch="master"

# add binary
ADD log-sink /log-sink

# add shell
ADD run.sh /run.sh
RUN chmod u+x /run.sh
RUN chmod u+x /log-sink
RUN mkdir -p /var/log/go_log


# Commands when creating a new container
CMD ["/run.sh"]