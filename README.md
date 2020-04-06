# Serverless data pipelines

## Architecture
![architecture.png](img/architecture.png)


## Instruction

* set your profile and region in `serverless.yml`
* tail logs: `serverless logs -f functionName`
* invoke: `serverless invoke --function functionName --data "hello world"`
* invoke with log: `serverless invoke --function functionName --data "hello world" --log`
* deploy: `sls deploy -v`
`