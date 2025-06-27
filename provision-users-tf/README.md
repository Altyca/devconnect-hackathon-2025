# Terraform Configuration

## Setup

Token from Workspace->Settings->Developer->Access Tokens

1. Copy the example secrets file:
   ```bash
   cp secrets.tfvars.example secrets.tfvars
   ```

2. Edit `secrets.tfvars` with your actual Databricks token:
   ```
   databricks_token = "your-actual-databricks-token"
   ```

3. Run Terraform commands with both variable files:
   ```bash
   terraform plan -var-file="terraform.tfvars" -var-file="secrets.tfvars"
   terraform apply -var-file="terraform.tfvars" -var-file="secrets.tfvars"
   ```

NOTE: email is sent to provisioned user

## Security

- `secrets.tfvars` is excluded from Git via `.gitignore`
- Never commit sensitive tokens to version control
- Use `secrets.tfvars.example` as a template for required variables 
