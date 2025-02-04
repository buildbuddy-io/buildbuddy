import json
import subprocess
import sys
import argparse
import time

def run(*args):
    proc = subprocess.run(args, encoding='utf-8', stdout=subprocess.PIPE, stderr=None, check=False)
    if proc.returncode != 0:
        print('Command', args, 'failed', file=sys.stderr)
        sys.exit(proc.returncode)
    return proc.stdout

def fatal(*args):
    print(*args, file=sys.stderr)
    exit(1)


def kube(context=''):
    def _call(*args):
        cmd = ['kubectl']
        if context:
            cmd.extend(['--context', context])
        cmd.extend(args)
        return run(*cmd)
    return _call

def get_pod_replica_set_owner(pod):
    for owner in pod.get('metadata', {}).get('ownerReferences', []):
        if owner.get('kind') == 'ReplicaSet':
            return owner
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--context', help='Kubernetes context', default='')
    parser.add_argument("-n", "--namespace", help="Kubernetes namespace", default="default")
    parser.add_argument("resource_type", help="deployment or statefulset")
    parser.add_argument("resource_name", help="Name of deployment or statefulset")
    args = parser.parse_args()

    kubectl = kube(context=args.context)

    def kubectl_json(*args):
        return json.loads(kubectl(*args, '-o', 'json'))

    # TODO: timeout?
    while True:
        # Get the current deployment revision
        deployment = kubectl_json('get', '-n', args.namespace, args.resource_type, args.resource_name)
        # TODO: deployment => statefulset ?
        current_revision = deployment.get('metadata', {}).get('annotations', {}).get('deployment.kubernetes.io/revision')
        if not current_revision:
            fatal('Missing revision annotation')
        app_label = deployment.get('spec', {}).get('selector', {}).get('matchLabels', {}).get('app')
        if not app_label:
            fatal('Missing app label')

        print('Current revision', current_revision)

        # List the ReplicaSets related to the deployment and find the one matching
        # the current revision
        replica_sets = kubectl_json('get', 'replicaset', '-n', args.namespace, '-l', 'app='+app_label)
        replica_sets_by_name = {}
        current_replica_set_name = None
        for rs in replica_sets.get('items', []):
            name = rs.get('metadata', {}).get('name')
            revision = rs.get('metadata', {}).get('annotations', {}).get('deployment.kubernetes.io/revision')
            if not revision:
                fatal('ReplicaSet', name, 'is missing revision')
            if revision == current_revision:
                current_replica_set_name = name
            replica_sets_by_name[name] = rs

        if current_replica_set_name is None:
            fatal('Could not find ReplicaSet corresponding to current revision')
        
        # List the pods and check whether any pods are not owned by the
        # replicaset with the latest revision
        pods = kubectl_json('get', 'pods', '-n', args.namespace, '-l', 'app='+app_label)
        outdated_pods = []
        for pod in pods.get('items', []):
            pod_replicaset_owner = get_pod_replica_set_owner(pod)
            if pod_replicaset_owner is None:
                fatal('Could not find pod ReplicaSet owner')
            if pod_replicaset_owner.get('name') != current_replica_set_name:
                # Pod is owned by an old replicaset and is outdated.
                outdated_pods.append(pod)
        
        if not outdated_pods:
            print('All running pods are on the new revision.')
            sys.exit(0)
        
        print('Outdated pods:')
        for pod in outdated_pods:
            rs = replica_sets_by_name.get(get_pod_replica_set_owner(pod).get('name'))
            pod_revision = rs.get('metadata', {}).get('annotations', {}).get('deployment.kubernetes.io/revision')
            print(pod.get('metadata', {}).get('name'), pod.get('status', {}).get('phase'), '@revision', pod_revision)
        
        time.sleep(5)
