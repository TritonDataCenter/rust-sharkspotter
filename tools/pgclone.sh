#!/bin/bash
#
# Copyright 2020 Joyent, Inc.
#
# This program runs from the Triton headnode and the caller specifies a Manatee
# VM uuid which will be cloned:
#
#   rebalancer-pgclone.sh <manatee VM UUID>
#
# and:
#
#  * creates a temporary (surrogate) VM using VMAPI
#  * creates a new manatee (zfs) snapshot and clones it
#  * attaches the cloned dataset as the delegated dataset for the surrogate VM
#  * installs a user-script which runs on startup and configures and starts postgresql
#  * starts the VM
#
# On any unexpected error it should exit prematurely with a non-zero exit code.
#

set -o errexit

if [[ -z $1 || -n $2 ]]; then
    echo "Usage: $0 <ma>" >&2
    exit 2
fi

VICTIM_UUID=$1

VICTIM_JSON="$(sdc-vmapi /vms/${VICTIM_UUID} | json -H)"
SERVER_UUID=$(json server_uuid <<<${VICTIM_JSON})

if [[ -z ${SERVER_UUID} ]]; then
    echo "FATAL: Failed to find server_uuid in VM object." 2>&1
    exit 1
fi

NEW_UUID=$(uuid -v4)
NEW_UUID_SHORT=$(cut -d'-' -f1 <<<${NEW_UUID})
NEW_ALIAS=$(json -e "this.alias = this.alias.replace(/\.postgres\./, '.rebalancer-postgres.').replace(/-[0-9a-f]+$/, '-${NEW_UUID_SHORT}')" alias <<<${VICTIM_JSON})

#
# Create the new payload with a bunch of properties first copied from the
# existing VM, and then some other properties are set specific to the clone.
#
# The new VM is provisioned on the same networks as the original, so we must use
# vmapi in order that the network interfaces are created correctly in the rest
# of Triton.
#
NEW_JSON=$(json -e 'this.new = {autoboot: false, billing_id: this.billing_id, brand: this.brand, cpu_shares: this.cpu_shares, customer_metadata: this.customer_metadata, delegate_dataset: true, dns_domain: this.dns_domain, image_uuid: this.image_uuid, max_locked_memory: this.max_locked_memory, max_lwps: this.max_lwps, max_physical_memory: this.max_physical_memory, max_swap: this.max_swap, owner_uuid: this.owner_uuid, quota: this.quota, ram: this.ram, resolvers: this.resolvers, server_uuid: this.server_uuid, tags: this.tags, tmpfs: this.tmpfs, zfs_io_priority: this.zfs_io_priority}' \
     -e 'this.new.cpu_cap = 0' \
     -e 'this.new.customer_metadata["user-script"] = "#!/usr/bin/bash\n#\nset -o xtrace\nset -o errexit\n\nif [[ -f /setup.sh ]]; then\n\t${DIR}/setup.sh\nfi\n"' \
     -e 'this.new.tags.manta_role = "rebalancer-postgres"' \
     -e "this.new.uuid = '${NEW_UUID}'" \
     -e "this.new.alias = '${NEW_ALIAS}'" \
     -e 'this.new.networks = this.nics.map(function _mapNic(n) { return {ipv4_uuid: n.network_uuid, mtu: n.mtu, nic_tag: n.nic_tag, primary: n.primary, vlan_id: n.vlan_id }})' \
     new <<<${VICTIM_JSON})

echo "Creating Surrogate VM ${NEW_UUID}"
echo "Payload:"
json <<<${NEW_JSON}

SNAP_NAME="rebalancer-$$"

#
# Provisioning with VMAPI is asynchronous so we get back a workflow job_uuid. We
# use sdc-waitforjob to poll the job status until it completes.
#
VMAPI_RESULT=$(sdc-vmapi /vms -X POST -d@- <<<${NEW_JSON} | json -H)
WF_JOB_UUID=$(json job_uuid <<<${VMAPI_RESULT})

sdc-waitforjob -t 600 ${WF_JOB_UUID}

#
# Helper function that just checks the sdc-oneachnode json results
function check_result {
    result_json=$1

    stdout="$(json stdout <<<${result_json})"
    stderr="$(json stderr <<<${result_json})"

    [[ -n ${stdout} ]] && echo "STDOUT: $stdout"
    [[ -n ${stderr} ]] && echo "STDERR: $stderr"
    if [[ $(json exit_status <<<${result_json}) -ne 0 ]]; then
        echo "Command failed:" >&2
        exit 1
    fi
}

#
# When we provision a new dataset is created. We don't need that one, so we
# destroy it. We'll replace it with a new clone of the manatee snapshot below.
#
echo "Destroying unused delegated dataset..."
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "zfs destroy zones/${NEW_UUID}/data" | json result)
check_result "${result_json}"

echo "Creating snapshot data/manate@${SNAP_NAME} on ${VICTIM_UUID}..."
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "zfs snapshot zones/${VICTIM_UUID}/data/manatee@${SNAP_NAME}" | json result)
check_result "${result_json}"

echo "Cloning snapshot data/manatee@${SNAP_NAME} from ${VICTIM_UUID} to zones/${NEW_UUID}/data..."
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "zfs clone zones/${VICTIM_UUID}/data/manatee@${SNAP_NAME} zones/${NEW_UUID}/data" | json result)
check_result "${result_json}"

#
# Keep a copy of manatee's registrar config so we can mangle it into something
# that works for rebalancer to find this instance.
#
echo "Copying registrar config  ${VICTIM_UUID} to ${NEW_UUID}..."
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "cp /zones/${VICTIM_UUID}/root/opt/smartdc/registrar/etc/config.json /zones/${NEW_UUID}/root/opt/smartdc/registrar/etc/config.json.in" | json result)
check_result "${result_json}"

#
# The user-script we setup earlier will run /setup.sh in the zone on startup. We
# create that with a script that will:
#
#  * setup postgres + permissions
#  * create a postgres service
#  * disable exisitng "recovery.conf" so that we don't attempt to recover from a
#    real manatee.
#  * import and startup the postgresql service
#

echo "Installing startup script..."

TMP_SCRIPT="/tmp/rebalancer-pgclone-setup.$$/setup.sh"
mkdir -p $(dirname ${TMP_SCRIPT})

cat >${TMP_SCRIPT} <<'EOS'
#!/bin/bash
#
# Copyright 2020 Joyent, Inc.
#
set -o xtrace
set -o errexit
set -o pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/opt/local/sbin:/opt/local/bin:/usr/sbin:/usr/bin:/sbin
hostname $(zonename)
data_dir="/zones/$(zonename)/data"
pg_version="$(json current < ${data_dir}/manatee-config.json)"
[[ -z $(grep "postgres::907" /etc/group) ]] && groupadd -g 907 postgres && useradd -u 907 -g postgres postgres
mkdir -p /var/pg
chown -R postgres:postgres /var/pg
# disable autovacuum and ensure we're not going to try to recover from a sync
grep -v "^autovacuum = " ${data_dir}/data/postgresql.conf > ${data_dir}/data/postgresql.conf.new
echo "autovacuum = off" >> ${data_dir}/data/postgresql.conf
[[ -f ${data_dir}/data/recovery.conf ]] && mv ${data_dir}/data/recovery.conf ${data_dir}/data/recovery.conf.disabled
PGBIN="/opt/postgresql/${pg_version}/bin"
PGDATA="${data_dir}/data"
# Copied from pkgsrc version and modified to fit
cat > pg.xml <<EOF
<?xml version='1.0'?>
<!DOCTYPE service_bundle SYSTEM '/usr/share/lib/xml/dtd/service_bundle.dtd.1'>
<service_bundle type='manifest' name='export'>
  <service name='manta/rebalancer-postgres' type='service' version='0'>
    <create_default_instance enabled='true'/>
    <single_instance/>
    <dependency name='network' grouping='require_all' restart_on='none' type='service'>
      <service_fmri value='svc:/milestone/network:default'/>
    </dependency>
    <dependency name='filesystem-local' grouping='require_all' restart_on='none' type='service'>
      <service_fmri value='svc:/system/filesystem/local:default'/>
    </dependency>
    <method_context working_directory='/var/pg'>
      <method_credential group='postgres' user='postgres'/>
      <method_environment>
        <envvar name='LD_PRELOAD_32' value='/usr/lib/extendedFILE.so.1'/>
        <envvar name='PATH' value='/opt/local/bin:/opt/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin'/>
      </method_environment>
    </method_context>
    <exec_method name='start' type='method' exec='${PGBIN}/pg_ctl -D ${PGDATA} -l /var/pg/postgresql.log start' timeout_seconds='300'/>
    <exec_method name='stop' type='method' exec='${PGBIN}/pg_ctl -D ${PGDATA} stop' timeout_seconds='300'/>
    <exec_method name='refresh' type='method' exec='${PGBIN}/pg_ctl -D ${PGDATA} reload' timeout_seconds='60'/>
    <template>
      <common_name>
        <loctext xml:lang='C'>PostgreSQL RDBMS</loctext>
      </common_name>
      <documentation>
        <manpage title='postgres' section='1M' manpath='/opt/local/man'/>
        <doc_link name='postgresql.org' uri='http://postgresql.org'/>
      </documentation>
    </template>
  </service>
</service_bundle>
EOF
svccfg import pg.xml
# Generate registrar config, then import and start the service
MY_IP=$(mdata-get sdc:nics | json -Ha nic_tag ip  | grep "^manta" | cut -d ' ' -f2)
if [[ -z "${MY_IP}" ]]; then
    echo "Unable to determine Manta IP" >&2
    exit 1
fi
# Ensure the domain looks like <shard>.moray.* since we depend on that in our mutation
if [[ -z $(json registration.domain < /opt/smartdc/registrar/etc/config.json.in | grep "^[0-9]*\.moray\." 2>/dev/null) ]]; then
    echo "Invalid config: bad registration.domain" >&2
    exit 1
fi
json -e "this.adminIp = '${MY_IP}'" \
    -e 'this.registration.aliases = [this.registration.domain.replace(/\.moray\./, ".rebalancer-postgres.")]' \
    -e 'this.registration.domain = this.registration.domain.replace(/^.*\.moray\./, "rebalancer-postgres.")' \
    > /opt/smartdc/registrar/etc/config.json < /opt/smartdc/registrar/etc/config.json.in
svccfg import /opt/smartdc/registrar/smf/manifests/registrar.xml
svcadm enable registrar
# Fix the prompt to be something more useful
alias="$(mdata-get sdc:alias)"
if [[ -n ${alias} ]]; then
cat >> /root/.bashrc <<EOF
export PS1="[\u@${alias} \w]\$ "
EOF
fi
EOS

# Upload the script into place
result_json=$(sdc-oneachnode -X -J -n "${SERVER_UUID}" -g ${TMP_SCRIPT} -d "/zones/${NEW_UUID}/root" | json result)
check_result "${result_json}"

# Fix the permissions
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "chmod 755 /zones/${NEW_UUID}/root/setup.sh" | json result)
check_result "${result_json}"

echo "Starting Surrogate VM..."
result_json=$(sdc-oneachnode -J -n "${SERVER_UUID}" "vmadm start ${NEW_UUID}" | json result)
check_result "${result_json}"

exit 0
