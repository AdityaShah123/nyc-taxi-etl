# Infrastructure Notes

This repository currently ships only application code and Spark binaries. No cloud IaC is committed. Use these guardrails when deploying:

- **S3 layout:**
	- `s3://<bucket>/tlc/raw/` for raw parquet inputs
	- `s3://<bucket>/tlc/curated/` for cleaned outputs
- **Permissions:** Principal running jobs needs `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` on the bucket prefix. Add `s3:DeleteObject` if cleanup is required.
- **Networking:** Allow outbound HTTPS to `s3.<region>.amazonaws.com`. If running on EMR/EC2, attach an IAM role with the S3 policy above.
- **Spark classpath:** Include `jars/hadoop-aws-3.4.1.jar` (or managed equivalent) in the driver and executor classpath for S3 access.
- **Configuration:** Set `fs.s3a.aws.credentials.provider` to the appropriate provider (environment variables, profile, or instance role). Enable server-side encryption if required by policy.
- **Logging/monitoring:** Forward Spark driver logs to CloudWatch (or equivalent) and capture application metrics if running in a managed cluster.

If infrastructure-as-code is added later, place Terraform/CloudFormation/CDK assets in this folder and keep state files out of version control.
