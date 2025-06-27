terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

locals {
  users_csv = csvdecode(file("${path.module}/users.csv"))
  users_map = {
    for user in local.users_csv : user.email => user
  }
}

# Create workspace users
resource "databricks_user" "workspace_users" {
  for_each = local.users_map

  user_name    = each.value.email
  display_name = each.value.display_name
  
  # Set additional user properties if needed
  active = true
  force = true
}

# All users get basic workspace access by default

# Optional: Add users to existing groups if needed
# Uncomment and modify the group names below if you want to assign users to existing groups
# 
# resource "databricks_group_member" "group_memberships" {
#   for_each = local.users_map
#
#   group_id  = "existing-group-id"  # Replace with actual group ID
#   member_id = databricks_user.workspace_users[each.key].id
# } 