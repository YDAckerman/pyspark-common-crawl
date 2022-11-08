terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
  profile = "udac_de_cap"
}

resource "aws_emr_cluster" "spark-cluster" {
  name          = "spark-cluster"
  release_label = "emr-5.36.0"
  # log_uri = "s3://capstone-bootstrap-logging"
  applications  = ["Hadoop", "Spark", "JupyterEnterpriseGateway",
    "Livy", "Hive"]
  
  ec2_attributes {
    subnet_id = "subnet-0ab4c2471136f469c"
    key_name = "spark-cluster"
    emr_managed_master_security_group = "sg-0b7e4b91a5074f36a"
    emr_managed_slave_security_group  = "sg-0b7e4b91a5074f36a"
    instance_profile                  = "EMR_EC2_DefaultRole"
  }
  
  master_instance_group {
    instance_type = "m3.xlarge"
    instance_count = 1
  }
  
  core_instance_group {
    instance_type = "m3.xlarge"
    instance_count = 2
  }

  bootstrap_action {
    path = "s3://capstone-emr-boostrap-action/bootstrap.sh"
    name = "my_bootstrap_actions"
  } 

  service_role = "EMR_DefaultRole"
}

