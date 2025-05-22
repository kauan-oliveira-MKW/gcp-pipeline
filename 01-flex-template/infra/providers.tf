provider "google" {

  project = var.project_id
  region  = var.region

}

terraform {

  backend "gcs" {

    bucket = "primeiro-bucket-20240514"
    prefix = "dataflow/templates/fpl-pipeline"

  }

  required_providers {

    docker = {

      source = "kreuzwerker/docker"

    }
  }
}
