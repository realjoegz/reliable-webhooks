# A small deployment: two nodes, one NAT gateway and a database without a standby. Copied to
# terraform.tfvars, which git ignores, these are the values that "terraform plan" reads.
# docs/deploy.md says what to hold them against before anything is applied.

region = "eu-west-1"

# A range that is reserved for documentation, in place of the one that kubectl and helm are run
# from. Without this line the API server of the cluster has no public endpoint, and inside the
# VPC it admits the nodes and no other machine.
cluster_endpoint_public_access_cidrs = ["203.0.113.0/24"]

single_nat_gateway = true

node_instance_types = ["t3.large"]
node_min_size       = 2
node_max_size       = 4

database_instance_class    = "db.t4g.medium"
database_allocated_storage = 20
database_multi_az          = false
