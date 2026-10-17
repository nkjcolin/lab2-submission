resource "terraform_data" "prerequisites" {
  lifecycle {
    precondition {
      condition     = !var.upload_input_file || fileexists(local.input_file_path)
      error_message = "twitter_combined.txt was not found. Set input_file_path or disable upload_input_file."
    }
  }
}
