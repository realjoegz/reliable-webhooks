config {
  # All but four resources are created inside the three modules that main.tf calls, so the
  # rules of the AWS plugin see an engine or an instance class only when they follow the values
  # into those modules. The sizes have no default, and the example gives the rules values to
  # follow.
  call_module_type = "all"
  varfile          = ["example.tfvars"]
}

plugin "terraform" {
  enabled = true
  preset  = "all"
}

plugin "aws" {
  enabled = true
  version = "0.49.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
