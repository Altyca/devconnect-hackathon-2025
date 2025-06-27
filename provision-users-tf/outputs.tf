output "provisioned_users" {
  description = "List of provisioned users"
  value = {
    for email, user in databricks_user.workspace_users : 
    email => {
      id           = user.id
      display_name = user.display_name
      active       = user.active
    }
  }
}

output "total_users_provisioned" {
  description = "Total number of users provisioned"
  value = length(local.users_map)
} 