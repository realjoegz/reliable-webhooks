variable "region" {
  description = "The AWS region of every resource."
  type        = string

  validation {
    condition     = can(regex("^[a-z]+(-[a-z]+)+-[0-9]+$", var.region))
    error_message = "The region must be the name of an AWS region, such as eu-west-1."
  }
}

variable "name" {
  description = "The name of the deployment. The VPC, the cluster, the node group and the database instance carry it, and two deployments in one region need two names."
  type        = string
  default     = "reliable-webhooks"

  # The form of an identifier of a database instance, which is the strictest of the names that
  # are made from this one.
  validation {
    condition     = can(regex("^[a-z][a-z0-9]*(-[a-z0-9]+)*$", var.name))
    error_message = "The name must start with a lowercase letter and hold lowercase letters, digits and single hyphens, without a hyphen at its end."
  }

  # The cluster module makes the prefix of the name of the IAM role of the node group from this
  # name and 16 characters of its own, and IAM takes a prefix of 38.
  validation {
    condition     = length(var.name) <= 22
    error_message = "The name must have at most 22 characters."
  }
}

variable "tags" {
  description = "Tags for every resource, next to Deployment and ManagedBy, which the module sets itself."
  type        = map(string)
  default     = {}

  validation {
    condition     = alltrue([for key in keys(var.tags) : !startswith(lower(key), "aws:")])
    error_message = "A tag key must not start with aws:, which AWS keeps for its own tags."
  }
}

variable "vpc_cidr" {
  description = "The IPv4 range of the VPC. Three sixteenths of it are the private subnets, where every node and every pod takes an address."
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr)) && try(cidrsubnet(var.vpc_cidr, 0, 0) == var.vpc_cidr, false)
    error_message = "The range must be an IPv4 network in CIDR notation, written with its network address, such as 10.0.0.0/16."
  }

  validation {
    condition     = try(tonumber(split("/", var.vpc_cidr)[1]) >= 16 && tonumber(split("/", var.vpc_cidr)[1]) <= 20, false)
    error_message = "The range must be between a /16 and a /20. A VPC is at most a /16, and a smaller range than a /20 leaves public subnets below the /28 that AWS accepts."
  }

  # The service connects to no address of these three ranges (section 11.4 of docs/design.md).
  # In a VPC outside them, a subscriber URL that resolves to a node, a pod or the database would
  # be a destination like any other.
  validation {
    condition     = can(regex("^(10|172\\.(1[6-9]|2[0-9]|3[01])|192\\.168)\\.", var.vpc_cidr))
    error_message = "The range must lie in 10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16."
  }
}

variable "single_nat_gateway" {
  description = "Whether the private subnets of all three zones share one NAT gateway. The deliveries to subscribers leave through it, so with one gateway they stop when its zone fails. Otherwise each zone has its own."
  type        = bool
  default     = false
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "The IPv4 ranges that reach the API server of the cluster from outside the VPC. With none, the API server has no public endpoint, and inside the VPC it admits the nodes and no other machine: kubectl and helm run there need a rule on the security group of the cluster, which this module does not create."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.cluster_endpoint_public_access_cidrs : can(cidrnetmask(cidr)) && try(cidrsubnet(cidr, 0, 0) == cidr, false)])
    error_message = "Each entry must be an IPv4 range in CIDR notation, written with its network address, such as 203.0.113.0/24."
  }
}

variable "node_instance_types" {
  description = "The instance types of the nodes. They have to be x86_64, because the node group runs the Amazon Linux 2023 image for that architecture."
  type        = list(string)

  validation {
    condition     = length(var.node_instance_types) > 0 && alltrue([for type in var.node_instance_types : can(regex("^[a-z0-9-]+\\.[a-z0-9-]+$", type))])
    error_message = "At least one instance type is needed, each written like t3.large."
  }
}

variable "node_min_size" {
  description = "The fewest nodes that the node group may have, and the number that it starts with."
  type        = number

  validation {
    condition     = var.node_min_size >= 1
    error_message = "The node group needs at least one node."
  }
}

variable "node_max_size" {
  description = "The most nodes that the node group may have. Nothing in this module adds a node."
  type        = number

  validation {
    condition     = var.node_max_size >= var.node_min_size
    error_message = "The largest number of nodes must not be below the smallest."
  }
}

variable "database_instance_class" {
  description = "The instance class of the database, such as db.t4g.medium. RDS derives max_connections from the memory of the class, and every replica of the service holds 32 connections."
  type        = string

  validation {
    condition     = can(regex("^db\\.[a-z0-9]+\\.[a-z0-9]+$", var.database_instance_class))
    error_message = "The instance class must be written like db.t4g.medium."
  }
}

variable "database_allocated_storage" {
  description = "The storage of the database in GiB. It is all that the instance has: the module does not let RDS add to it."
  type        = number

  validation {
    condition     = var.database_allocated_storage >= 20
    error_message = "The storage must be at least 20 GiB, the least that RDS gives a gp3 volume."
  }
}

variable "database_multi_az" {
  description = "Whether the database has a standby in a second zone, which RDS fails over to. Without one, the database is gone for as long as its zone or its instance is."
  type        = bool
  default     = true
}

variable "database_backup_retention_days" {
  description = "The number of days that RDS keeps the automated backups of the database, and so how far back it can be restored to a point in time."
  type        = number
  default     = 7

  validation {
    condition     = var.database_backup_retention_days >= 1 && var.database_backup_retention_days <= 35
    error_message = "The retention must be from 1 to 35 days. RDS keeps backups no longer, and 0 would turn them off."
  }
}
