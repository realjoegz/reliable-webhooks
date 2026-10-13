# The module without an AWS account. Every run is a plan against a provider that calls nothing,
# with the values of example.tfvars unless the run names others:
#
#   terraform test -var-file=example.tfvars
#
# The runs show that the example and the defaults of the module are planned, that the outputs
# have the form that the Helm chart takes, that the rotation of the database password is turned
# off, and that the variables refuse what their messages say. They do not show that AWS accepts
# the plan.

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
    }
  }

  # A mocked provider answers a data source with random strings, and the provider refuses a
  # resource whose ARN or policy document does not have the form of one. These are the four
  # answers that the cluster module builds such values from.
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      arn = "arn:aws:iam::000000000000:user/applies-the-module"
    }
  }

  mock_data "aws_iam_session_context" {
    defaults = {
      issuer_arn = "arn:aws:iam::000000000000:user/applies-the-module"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{}"
    }
  }

  # RDS names the endpoint of an instance and the secret of its master user once the instance
  # exists. With both known to the plan, the run below can read the outputs that are built from
  # them, and the secret whose rotation is turned off. The mocked provider answers with a secret
  # whatever the instance is planned with, so no run shows that RDS manages the password.
  mock_resource "aws_db_instance" {
    override_during = plan

    defaults = {
      endpoint = "reliable-webhooks.abcdefghijkl.eu-west-1.rds.amazonaws.com:5432"
      master_user_secret = [{
        secret_arn = "arn:aws:secretsmanager:eu-west-1:000000000000:secret:rds!db-00000000-0000-0000-0000-000000000000-abcdef"
      }]
    }
  }
}

# The security group that the cluster module creates for the nodes. With an id that is known to
# the plan, a run can tell it from every other source that the rule of the database could name.
override_resource {
  target          = module.eks.aws_security_group.node
  override_during = plan

  values = {
    id = "sg-nodes"
  }
}

run "example_is_planned" {
  command = plan

  assert {
    condition     = module.vpc.private_subnets_cidr_blocks == tolist(["10.0.0.0/20", "10.0.16.0/20", "10.0.32.0/20"])
    error_message = "The private subnets are not three sixteenths of the default range."
  }

  assert {
    condition     = module.vpc.public_subnets_cidr_blocks == tolist(["10.0.48.0/24", "10.0.49.0/24", "10.0.50.0/24"])
    error_message = "The public subnets do not follow the private ones."
  }

  assert {
    condition     = length(module.vpc.natgw_ids) == 1
    error_message = "The three zones of the example have to share one NAT gateway."
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.database_from_nodes.ip_protocol == "tcp"
      && aws_vpc_security_group_ingress_rule.database_from_nodes.from_port == 5432
      && aws_vpc_security_group_ingress_rule.database_from_nodes.to_port == 5432
      && aws_vpc_security_group_ingress_rule.database_from_nodes.referenced_security_group_id == "sg-nodes"
      && aws_vpc_security_group_ingress_rule.database_from_nodes.cidr_ipv4 == null
      && aws_vpc_security_group_ingress_rule.database_from_nodes.cidr_ipv6 == null
      && aws_vpc_security_group_ingress_rule.database_from_nodes.prefix_list_id == null
    )
    error_message = "The database has to admit TCP port 5432 from the security group of the nodes and from nothing else."
  }

  assert {
    condition = (
      { for parameter in aws_db_parameter_group.database.parameter : parameter.name => parameter.value }
      == { default_transaction_isolation = "read committed", synchronous_commit = "on" }
    )
    error_message = "The parameter group has to set the two settings that a replica checks when it starts, and no other."
  }

  assert {
    condition     = output.cluster_name == "reliable-webhooks"
    error_message = "The cluster does not have the default name."
  }

  # The schema of the chart takes a URL that begins with jdbc:postgresql:// and carries no
  # password. The parameter of this one tells the driver to connect over TLS or not at all.
  assert {
    condition     = output.database_url == "jdbc:postgresql://reliable-webhooks.abcdefghijkl.eu-west-1.rds.amazonaws.com:5432/webhooks?sslmode=require"
    error_message = "The URL of the database is ${output.database_url}."
  }

  assert {
    condition     = output.database_username == "webhooks"
    error_message = "The database user is not the one that the module creates."
  }

  assert {
    condition     = output.database_password_secret_arn == "arn:aws:secretsmanager:eu-west-1:000000000000:secret:rds!db-00000000-0000-0000-0000-000000000000-abcdef"
    error_message = "The output has to name the secret in which RDS keeps the password of the master user."
  }

  # RDS rotates the password in that secret for as long as the rotation of the secret is on, and
  # the Secret of the chart would keep the old one. main.tf can give the rotation a schedule in
  # two places: in its own resource, which has to turn the rotation off, and through the RDS
  # module, whose output is null as long as the module schedules none.
  assert {
    condition = (
      aws_secretsmanager_secret_rotation.database.secret_id == output.database_password_secret_arn
      && aws_secretsmanager_secret_rotation.database.rotation_enabled == false
      && length(aws_secretsmanager_secret_rotation.database.rotation_rules) == 0
      && module.rds.db_instance_secretsmanager_secret_rotation_enabled == null
    )
    error_message = "The rotation of the secret that holds the password of the database has to be turned off, without a schedule."
  }
}

# The example replaces three defaults of variables.tf, and this run has them again: an API
# server without a public endpoint, a NAT gateway in each zone and a database with a standby.
run "defaults_that_the_example_replaces_are_planned" {
  command = plan

  variables {
    cluster_endpoint_public_access_cidrs = []
    single_nat_gateway                   = false
    database_multi_az                    = true
  }

  assert {
    condition     = length(module.vpc.natgw_ids) == 3
    error_message = "Each of the three zones has to have a NAT gateway of its own."
  }
}

run "tags_of_the_module_win" {
  command = plan

  variables {
    tags = {
      Team      = "platform"
      ManagedBy = "hand"
    }
  }

  assert {
    condition = (
      length(local.tags) == 3
      && local.tags["Team"] == "platform"
      && local.tags["Deployment"] == "reliable-webhooks"
      && local.tags["ManagedBy"] == "terraform"
    )
    error_message = "A tag of the deployment has to stay, and ManagedBy has to be the one of the module."
  }
}

run "values_at_the_lower_limits_are_accepted" {
  command = plan

  variables {
    vpc_cidr                       = "172.31.16.0/20"
    node_min_size                  = 1
    node_max_size                  = 1
    database_allocated_storage     = 20
    database_backup_retention_days = 1
  }

  assert {
    condition     = module.vpc.private_subnets_cidr_blocks == tolist(["172.31.16.0/24", "172.31.17.0/24", "172.31.18.0/24"])
    error_message = "The private subnets are not three sixteenths of a /20."
  }

  assert {
    condition     = module.vpc.public_subnets_cidr_blocks == tolist(["172.31.19.0/28", "172.31.19.16/28", "172.31.19.32/28"])
    error_message = "The public subnets of a /20 are not the /28 that AWS still accepts."
  }
}

run "values_at_the_upper_limits_are_accepted" {
  command = plan

  variables {
    name                           = "a-name-22-characters-x"
    vpc_cidr                       = "10.0.0.0/16"
    database_backup_retention_days = 35
  }
}

run "values_below_the_lower_limits_are_refused" {
  command = plan

  variables {
    vpc_cidr                       = "172.31.16.0/21"
    node_instance_types            = []
    node_min_size                  = 0
    database_allocated_storage     = 19
    database_backup_retention_days = 0
  }

  expect_failures = [
    var.vpc_cidr,
    var.node_instance_types,
    var.node_min_size,
    var.database_allocated_storage,
    var.database_backup_retention_days,
  ]
}

run "values_above_the_upper_limits_are_refused" {
  command = plan

  variables {
    name                           = "a-name-23-characters-xy"
    vpc_cidr                       = "10.0.0.0/15"
    database_backup_retention_days = 36
  }

  expect_failures = [
    var.name,
    var.vpc_cidr,
    var.database_backup_retention_days,
  ]
}

run "largest_number_of_nodes_below_the_smallest_is_refused" {
  command = plan

  variables {
    node_min_size = 3
    node_max_size = 2
  }

  expect_failures = [
    var.node_max_size,
  ]
}

# The region of the AWS European Sovereign Cloud, whose first part has four letters, and two
# instance types with a hyphen.
run "names_with_longer_parts_are_accepted" {
  command = plan

  variables {
    region              = "eusc-de-east-1"
    node_instance_types = ["m7i-flex.large", "m7i.metal-24xl"]
  }
}

run "values_of_the_wrong_form_are_refused" {
  command = plan

  variables {
    region = "Ireland"
    name   = "webhooks-"
    tags = {
      "aws:team" = "platform"
    }
    vpc_cidr                             = "10.0.1.0/16"
    cluster_endpoint_public_access_cidrs = ["203.0.113.7/24"]
    node_instance_types                  = ["t3large"]
    database_instance_class              = "t4g.medium"
  }

  expect_failures = [
    var.region,
    var.name,
    var.tags,
    var.vpc_cidr,
    var.cluster_endpoint_public_access_cidrs,
    var.node_instance_types,
    var.database_instance_class,
  ]
}

# Both ranges are written as the rules of form ask. The first is public address space, and the
# second is IPv6.
run "ranges_of_the_wrong_kind_are_refused" {
  command = plan

  variables {
    vpc_cidr                             = "172.32.0.0/16"
    cluster_endpoint_public_access_cidrs = ["2001:db8::/32"]
  }

  expect_failures = [
    var.vpc_cidr,
    var.cluster_endpoint_public_access_cidrs,
  ]
}

run "region_with_two_zones_is_refused" {
  command = plan

  override_data {
    target = data.aws_availability_zones.available

    values = {
      names = ["eu-west-1a", "eu-west-1b"]
    }
  }

  expect_failures = [
    data.aws_availability_zones.available,
  ]
}
