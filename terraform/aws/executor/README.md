# BuildBuddy Executor Deployment

BuildBuddy Executor is a tool that allows you to speed up your builds by running them on Kubernetes.

This terraform help you deploy BuildBuddy Executor separately from the BuildBuddy App deployment.

## How to use

Follow these simple steps to get started:

### 1. Setting up BuildBuddy:

a. Deploy BuildBuddy Enterprise (optional) by following the instructions [here](../README.md).

b. Navigate to BuildBuddy WebUI Org API Keys at `https://<BUILDBUDDY_URL>/settings/org/api-keys`.

c. Create a new API Key for self-hosted Executor by selecting **Executor key**.

### 2. Prepare your kubernetes cluster:

a. You can use the EKS cluster that you used to deploy BuildBuddy Enterprise in Step 1 or create a separate cluster.

b. Make sure that your target nodes have enough resources.

If you prefer to provision a separate EKS cluster for BuildBuddy Executor, please contact us.

### 3. Prepare terraform deployment

a. Create a `deployments.tfvars` file and fill in the information below using outputs of `aws eks list-clusters` and `aws eks describe-cluster --name <cluster_name>`

```terraform
cluster_name = "<cluster_name>"
cluster_endpoint = "https://<cluster_endpoint>.eks.amazonaws.com"
cluster_ca_certificate = "<CA_CERT>"

buildbuddy_api_key = "<BUILDBUDDY_API_KEY>"
buildbuddy_app_target = "grpcs://<BUILDBUDDY_URL>"
buildbuddy_pool = "<executor_pool_name>"
```

b. Run `terraform apply -var-file='deployments.tfvars'`

---

That's it! Your BuildBuddy Executor is now ready to run builds on Kubernetes. If you have any issues or questions, don't hesitate to contact us.
