# GCP Terraform Configs

This directory contains terraform configuration files to bring up a new
BuildBuddy cluster in a production ready state.

## About

These configs are very opinionated -- they contain the state of an ideal
cluster made for use with BuildBuddy Enterprise @ HEAD. This configuration
is suitable for serving production traffic: it is high-performance and includes
redundancy in case machines fail.

## Usage

To use these configs, you'll need the `terraform` command installed.

 * Create a new GCP project and get a service account json file.
 * Grant quota for x, y, and z.
 * Enable the following APIs:
 ```bash
	 gcloud services enable container.googleapis.com
	 gcloud services enable domains.googleapis.com
	 gcloud services enable dns.googleapis.com
     gcloud services enable servicenetworking.googleapis.com
 ```
 * Create a new tfvars file from the template and enter  your cluster
   details.
 * Run:
 ```bash
   terraform apply -var-file=your-cluster.tfvars
 ```
 * Profit

## Development

 * TBD
 * Do use sub-modules.
 * Don't check in secrets.
 * Don't add a bunch of optional configuration -- we want to focus our testing
   on a single happy path.

## Resources

 * https://www.terraform-best-practices.com/code-structure
 * https://medium.com/codex/terraform-best-practices-how-to-structure-your-terraform-projects-b5b050eab554
