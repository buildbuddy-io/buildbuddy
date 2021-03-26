module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  cluster_name    = local.cluster_name
  cluster_version = "1.18"
  subnets         = module.vpc.private_subnets

  tags = {
    Environment = "buildbuddy"
  }

  vpc_id = module.vpc.vpc_id

  workers_group_defaults = {
    root_volume_type = "gp2"
  }

  worker_groups = [
    {
      name                          = "buildbuddy-worker-group"
      instance_type                 = "m5.2xlarge"
      additional_security_group_ids = [aws_security_group.buildbuddy_security_group.id]
      asg_desired_capacity          = var.nodes
      asg_min_size                  = var.nodes
      asg_max_size                  = var.nodes
    },
  ]
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}
