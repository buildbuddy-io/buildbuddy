module "ebs_csi_irsa_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "5.10.0"

  role_name             = "ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    ex = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}

module "ebs_csi_driver_controller" {
  source  = "DrFaust92/ebs-csi-driver/kubernetes"
  version = "3.5.0"

  oidc_url = module.eks.oidc_provider


  depends_on = [
    null_resource.update-kubeconfig
  ]
}
