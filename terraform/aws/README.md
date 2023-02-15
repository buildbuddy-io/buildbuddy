# BuildBuddy Example Deployment on AWS with Terraform

This is **Experimental**, DO NOT USE!

## Getting Started

1. Setup AWS CLI

```
> brew install awscli

> aws configure
```

2. Export appropriate variables

```
> echo 'export KUBE_CONFIG_PATH=~/.kube/config' >> ~/.zshrc

> source ~/.zshrc
```

3. Run Terraform

```
> brew install terraform

> cd buildbuddy/terraform/aws

> terraform init

> terraform apply
```

4. Set kubectl context

```
> aws eks list-clusters

> aws eks --region $(terraform output -raw region) update-kubeconfig \
          --name $(terraform output -raw cluster_name)

# Check everything is working
> kubectl cluster-info
> kubectl get nodes
> kubectl get pods

# Get External URL to access BuildBuddy service
> kubectl get service
```

5. Clean up

```
> terraform destroy

# Our PVC were claimed by Helm chart, but helm does not delete PVC upon
# reversal, so we should manually clean them up.
#
# See https://github.com/helm/helm/issues/5156 for more information.

> aws ec2 describe-volumes
> aws ec2 delete-volume --volume-id <vol-id>
```

## Known problems

1. Terraform destroy got stuck at VPC

   This is most likely due to some eni is still being used and cannot be destroyed.
   Upon further investigation, it's likely that the ordering of the helm chart deletion left the EC2 Load Balancer in-place.
   These Load Balancer, in turn, blocks the deletion of ENI and thus, blocks VPC destroy.

   Fix: Go to AWS Console and manually delete the Load Balancer

1. Left over resources after Terraform destroy

   It's very likely that there are left over resources that require manual clean up

   For example:

   - EBS volumes are left over helm avoids cleaning up PVC (see 'Clean up' section above)
   - DHCP option set was automatically created with the VPC, but isn't tracked by Terraform
