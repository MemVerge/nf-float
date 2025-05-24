#!/bin/bash
#
# This script can be submitted as a contianerInit hook script to do the following,
#
# - Install nextflow software and its dependencies
# - If running as admin, a 'nextflow' admin user is added with /home/nextflow home directory
# - If running as a non-admin user, the user is created with the same UID as the MM Cloud user with /home/$USER home directory
# - Makes the FSx and EFS directories writable by all users
#
# Running as admin can cause Nextflow to fail with permission denied errors
#
# Following parameters can be passed in as environment variables:
#
# OPCENTER_PASSWORD_SECRET: The name of the OPCentre secret passed as '{secret:<Secret name>}'.
# FSX_MOUNT_PATH: The mount path for FSx. Default is '/mnt/fsx'.
# EFS_MOUNT_PATH: The mount path for EFS. Default is '/mnt/efs'.

#set -x
OPCENTER_PASSWORD_SECRET=${OPCENTER_PASSWORD_SECRET:-'{secret:OPCENTER_PASSWORD}'}
FSX_MOUNT_PATH=${FSX_MOUNT_PATH:-'/mnt/fsx'}
EFS_MOUNT_PATH=${EFS_MOUNT_PATH:-'/mnt/efs'}

export PATH=$PATH:/usr/bin:/usr/local/bin:/opt/memverge/bin
export HOME=/root
export HOME_DIR="/home"
export NF_ROOT_HOME="$HOME_DIR/nextflow"

LOG_FILE=$FLOAT_JOB_PATH/container-init.log
touch $LOG_FILE
exec >$LOG_FILE 2>&1

function log() {
  if [[ -f ${LOG_FILE_PATH} ]]; then
    echo $(date): "$@" >>${LOG_FILE_PATH}
  fi
  echo $(date): "$@"
}

function error() {
  log "[ERROR] $1"
}

function die() {
  error "$1"
  podman kill -a 2>&1 >/dev/null
  exit 1
}

function trim_quotes() {
  : "${1//\'/}"
  printf '%s\n' "${_//\"/}"
}

function assure_root() {
  if [[ ${EUID} -ne 0 ]]; then
    die "Please run with root or sudo privilege."
  fi
}

function echolower {
  tr [:upper:] [:lower:] <<<"${*}"
}

function get_secret {
  input_string=$1

  pattern='^\{secret:(.*)\}$'

  if [[ $input_string =~ $pattern ]]; then
    # Matched, return the secret name string
    matched_string="${BASH_REMATCH[1]}"
    secret_value=$(float secret get $matched_string -a $FLOAT_ADDR)
    if [[ $? -eq 0 ]]; then
      # Have this secret, will use the secret value
      echo $secret_value
      return
    else
      # Don't have this secret, will still use the input string
      echo $1
    fi
  else
    # Not matched, return the input string
    echo $1
  fi
}

function set_secret {
  file_name=$1
  secret_name=${FLOAT_JOB_ID}_SSHKEY
  float secret set $secret_name --file $file_name -a $FLOAT_ADDR
  if [[ $? -ne 0 ]]; then
    die "Set secret $secret_name failed"
  fi
}

function install_java() {
  java_path=$(which java)
  if [[ $? -eq 0 ]]; then
    log "Java is already installed at $java_path"
    return
  fi
  log "Install java"
  yum install -y --quiet java
  if [[ $? -ne 0 ]]; then
    die "Install java failed"
  fi
}

function install_git() {
  git_path=$(which git)
  if [[ $? -eq 0 ]]; then
    log "Git is already installed at $git_path"
    return
  fi
  log "Install git"
  yum install -y --quiet git
  if [[ $? -ne 0 ]]; then
    die "Install git failed"
  fi
}

function install_tmux() {
  tmux_path=$(which tmux)
  if [[ $? -eq 0 ]]; then
    log "Tmux is already installed at $tmux_path"
    return
  fi
  log "Install Tmux"
  yum install -y --quiet tmux
  if [[ $? -ne 0 ]]; then
    die "Install Tmux failed"
  fi
}

function install_nextflow() {
  export PATH=$PATH:/usr/bin
  nf_path=$(which nextflow)
  if [[ $? -eq 0 ]]; then
    log "Nextflow is already installed at $nf_path"
    return
  fi
  log "Install nextflow"
  curl -s https://get.nextflow.io | bash
  if [[ $? -ne 0 ]]; then
    die "Install nextflow failed"
  fi
  mv nextflow /usr/local/bin
  chmod 755 /usr/local/bin/nextflow
}

function prepare_user_env {
  if [[ $FLOAT_USER_ID -eq 0 ]]; then
    /usr/sbin/useradd -m -d $NF_ROOT_HOME -s /bin/bash nextflow
    su - nextflow -c "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa > /dev/null"
    su - nextflow -c "mv ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys"
    set_secret $NF_ROOT_HOME/.ssh/id_rsa
    rm -f $NF_ROOT_HOME/.ssh/id_rsa
    USER_PROFILE=$NF_ROOT_HOME/.bash_profile
    echo "nextflow ALL=(ALL:ALL) NOPASSWD: ALL" | tee -a /etc/sudoers.d/nextflow
  else
    systemctl stop munge
    /usr/sbin/userdel slurm
    /usr/sbin/userdel munge
    id $FLOAT_USER_ID > /dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      old_name=`getent passwd $FLOAT_USER_ID | cut -d: -f1`
      /usr/sbin/userdel $old_name
    fi
    FLOAT_USER_HOME="$HOME_DIR/$FLOAT_USER"
    /usr/sbin/useradd -u $FLOAT_USER_ID -m -d $FLOAT_USER_HOME -s /bin/bash $FLOAT_USER
    su - $FLOAT_USER -c "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa > /dev/null"
    su - $FLOAT_USER -c "mv ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys"
    set_secret $FLOAT_USER_HOME/.ssh/id_rsa
    rm -f $FLOAT_USER_HOME/.ssh/id_rsa
    USER_PROFILE=$FLOAT_USER_HOME/.bash_profile
  fi
}

function make_fsx_efs_writable {

  if [ -d $FSX_MOUNT_PATH ]; then
    chmod 777 $FSX_MOUNT_PATH
    log "Made FSx at $FSX_MOUNT_PATH writable"
  fi

  if [ -d $EFS_MOUNT_PATH ]; then
    chmod 777 $EFS_MOUNT_PATH
    log "Made EFS at $EFS_MOUNT_PATH writable"
  fi
}

function login_to_mmc {
  log "Login to MMC"
  if [[ $FLOAT_USER_ID -eq 0 ]]; then
    log "su - nextflow -c float login -a $FLOAT_ADDR -u $FLOAT_USER -p ****"
    su - nextflow -c "float login -a $FLOAT_ADDR -u $FLOAT_USER -p $(get_secret $OPCENTER_PASSWORD_SECRET)"
  else
    log "su - $FLOAT_USER -c float login -a $FLOAT_ADDR -u $FLOAT_USER -p ****"
    su - $FLOAT_USER -c "float login -a $FLOAT_ADDR -u $FLOAT_USER -p $(get_secret $OPCENTER_PASSWORD_SECRET)"
  fi
}

#env

assure_root

install_tmux
install_java
install_git
install_nextflow

prepare_user_env
make_fsx_efs_writable
login_to_mmc

exit 0
