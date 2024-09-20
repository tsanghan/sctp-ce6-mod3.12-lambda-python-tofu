terraform {
  backend "s3" {
    bucket = "sctp-ce6-tfstate"
    key    = "tsanghan-ce6-mod3_12-Lambda-Python-Tofu.tfstate"
    region = "ap-southeast-1"
  }
}