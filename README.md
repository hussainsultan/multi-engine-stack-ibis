## README
This repository accompanies the blog post [here](https://www.letsql.com/posts/multi-engine-data-stack-ibis/).

## Credentials
Please edit `.envrc.snowflake` to configure the account, warehouse and schema. 

The user credentials should be in a plaintext file that is **NOT** in version control. The file should be called `.envrc.snowflake.user` and have the following format
```
export SNOWFLAKE_USER=
export SNOWFLAKE_PASSWORD=
```
