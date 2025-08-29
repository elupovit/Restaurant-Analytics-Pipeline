########################################
# Provider Configuration
########################################
provider "aws" {
  region = var.aws_region
}

########################################
# Variables
########################################
variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "bronze_to_silver_job_name" {
  description = "Glue job name for Bronze to Silver ETL"
  type        = string
  default     = "bronze-to-silver-job"
}

variable "silver_to_gold_job_name" {
  description = "Glue job name for Silver to Gold ETL"
  type        = string
  default     = "silver-to-gold-job"
}

variable "scheduler_role_arn" {
  description = "IAM role ARN for EventBridge Scheduler to invoke Glue"
  type        = string
  default     = "arn:aws:iam::377318185071:role/EventBridgeSchedulerGlueRole"
}

########################################
# EventBridge Scheduler - Bronze to Silver
########################################
resource "aws_scheduler_schedule" "bronze_to_silver_yearly" {
  name                         = "bronze-to-silver-yearly"
  group_name                   = "default"
  state                        = "ENABLED"
  schedule_expression          = "cron(0 0 1 1 ? *)" # Runs once a year on Jan 1, midnight UTC
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = var.scheduler_role_arn

    input = jsonencode({
      JobName = var.bronze_to_silver_job_name
    })
  }
}

########################################
# Glue Trigger - Silver after Bronze
########################################
resource "aws_glue_trigger" "silver_after_bronze" {
  name     = "trigger-silver-after-bronze"
  type     = "CONDITIONAL"
  enabled  = true

  actions {
    job_name = var.silver_to_gold_job_name
  }

  predicate {
    logical = "AND"

    conditions {
      job_name         = var.bronze_to_silver_job_name
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }
}
