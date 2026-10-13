# The network and the cluster that the Helm chart of this repository is installed on, and the
# database that the chart is pointed at. docs/deploy.md says what to check before this is
# applied, and how the outputs become values of the chart.

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = merge(var.tags, {
    Deployment = var.name
    ManagedBy  = "terraform"
  })

  zones = slice(data.aws_availability_zones.available.names, 0, 3)

  # Six consecutive pieces of the range of the VPC. A private subnet is a sixteenth of it and a
  # public one a sixteenth of that: a pod takes an address of the subnet of its node, and the
  # public subnets hold the NAT gateways and nothing else.
  subnets         = cidrsubnets(var.vpc_cidr, 4, 4, 4, 8, 8, 8)
  private_subnets = slice(local.subnets, 0, 3)
  public_subnets  = slice(local.subnets, 3, 6)

  database_name     = "webhooks"
  database_username = "webhooks"
  database_port     = 5432
}

data "aws_availability_zones" "available" {
  state = "available"

  # Leaves out local zones and Wavelength zones, which an account has to opt in to.
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }

  lifecycle {
    postcondition {
      condition     = length(self.names) >= 3
      error_message = "The region must have at least three availability zones."
    }
  }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.7.3"

  name = var.name
  cidr = var.vpc_cidr
  azs  = local.zones

  private_subnets = local.private_subnets
  public_subnets  = local.public_subnets

  # The nodes have no public address. What they pull and what the service sends to subscribers
  # leaves through a NAT gateway in a public subnet.
  enable_nat_gateway = true
  single_nat_gateway = var.single_nat_gateway
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.29.0"

  name = var.name

  # The version of Kubernetes that CI installs the chart on, in the smoke test on kind. The node
  # group has the version of its cluster.
  kubernetes_version = "1.37"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  endpoint_public_access       = length(var.cluster_endpoint_public_access_cidrs) > 0
  endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs

  # Without this the cluster module gives nobody access to the cluster that it creates. With
  # it, whoever applies this is an administrator of the cluster.
  enable_cluster_creator_admin_permissions = true

  # The cluster module creates a cluster without the add-ons that EKS would install by itself,
  # so the three that every node needs are named here. A node is not Ready before it has a pod
  # network, so that add-on comes ahead of the node group. Each has the version that EKS names
  # as the default for the Kubernetes version, and not the newest.
  addons = {
    vpc-cni = {
      before_compute = true
      most_recent    = false
    }
    kube-proxy = {
      most_recent = false
    }
    coredns = {
      most_recent = false
    }
  }

  eks_managed_node_groups = {
    (var.name) = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.node_instance_types

      min_size = var.node_min_size
      max_size = var.node_max_size

      # Read when the node group is created. The cluster module ignores it from then on, so
      # the number of nodes can be changed between the two limits without Terraform changing it
      # back.
      desired_size = var.node_min_size
    }
  }

  # The instances, volumes and network interfaces of the nodes are tagged by the launch template
  # of the node group, which the default tags of the provider do not reach.
  tags = local.tags
}

resource "aws_security_group" "database" {
  name        = "${var.name}-database"
  description = "The database of ${var.name}. Admits the nodes of its cluster and nothing else."
  vpc_id      = module.vpc.vpc_id

  tags = {
    Name = "${var.name}-database"
  }
}

# The pods of the cluster have the security group of their node, so this is the rule that lets
# the replicas of the service in.
resource "aws_vpc_security_group_ingress_rule" "database_from_nodes" {
  security_group_id = aws_security_group.database.id
  description       = "PostgreSQL from the nodes of the cluster"

  referenced_security_group_id = module.eks.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = local.database_port
  to_port                      = local.database_port
}

# The two settings of the database that a replica checks when it starts, and refuses to start
# without (section 15 of docs/design.md). The group is a resource of this module and not of the
# RDS module, so that the plan test can read what it sets.
resource "aws_db_parameter_group" "database" {
  name_prefix = "${var.name}-"
  description = "The settings of the database of ${var.name} that the service checks when it starts."
  family      = "postgres18"

  parameter {
    name  = "default_transaction_isolation"
    value = "read committed"
  }

  parameter {
    name  = "synchronous_commit"
    value = "on"
  }

  # RDS does not delete a group that an instance uses. A group that has to be replaced is
  # therefore created first, under another name with the same prefix.
  lifecycle {
    create_before_destroy = true
  }
}

module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "7.2.2"

  identifier = var.name

  # The version that the tests of the service run against. Another minor version is a change to
  # this file, so RDS does not move the instance to one by itself.
  engine                     = "postgres"
  engine_version             = "18.6"
  auto_minor_version_upgrade = false

  instance_class    = var.database_instance_class
  allocated_storage = var.database_allocated_storage
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = local.database_name
  username = local.database_username
  port     = local.database_port

  # RDS creates the password of that user and keeps it in Secrets Manager. It is in no variable
  # and not in the state.
  manage_master_user_password = true

  multi_az               = var.database_multi_az
  create_db_subnet_group = true
  subnet_ids             = module.vpc.private_subnets
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false

  backup_retention_period = var.database_backup_retention_days
  deletion_protection     = true

  create_db_parameter_group = false
  parameter_group_name      = aws_db_parameter_group.database.name
}

# RDS rotates a password that it manages every seven days, and the Secret of the chart holds a
# copy that nothing refreshes. The rotation is therefore turned off, and the password changes
# only when it is rotated by hand, as docs/deploy.md describes. The RDS module turns a rotation
# off only by removing a schedule that an earlier apply has set, so this is a resource of this
# module.
resource "aws_secretsmanager_secret_rotation" "database" {
  secret_id        = module.rds.db_instance_master_user_secret_arn
  rotation_enabled = false
}
