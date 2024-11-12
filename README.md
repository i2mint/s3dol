# s3dol

s3 (through boto3) with a simple (dict-like or list-like) interface

To install:	```pip install s3dol```

[Documentation](https://i2mint.github.io/s3dol/)


## Set up credentials

Recommended prerequisite to make getting started easier but not required.

### Option 1: [Environment Variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)
```
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

### Option 2: [Configure Default Profile in Credentials File](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
Add credentails in `~/.aws/credentials`
```
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Option 3: [Configure Default Profile with AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/configure/)
[Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
```bash
brew instal awscli
```
Set credentails with CLI
```bash
aws configure
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-west-2
Default output format [None]:
```
