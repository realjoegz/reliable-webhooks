output "cluster_name" {
  description = "The name of the cluster, which `aws eks update-kubeconfig --name` takes."
  value       = module.eks.cluster_name
}

output "database_url" {
  description = "The value `database.url` of the Helm chart."
  value       = "jdbc:postgresql://${module.rds.db_instance_endpoint}/${local.database_name}?sslmode=require"
}

output "database_username" {
  description = "The value `database.username` of the Helm chart."
  value       = local.database_username
}

output "database_password_secret_arn" {
  description = "The secret in Secrets Manager in which RDS keeps the password of that user, as the member `password`. The key `database-password` of the Secret of the chart is a copy of it."
  value       = module.rds.db_instance_master_user_secret_arn
}
