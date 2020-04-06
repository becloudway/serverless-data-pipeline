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

## Data

* configuration of measurement locations: [http://miv.opendata.belfla.be/miv/configuratie/xml](http://miv.opendata.belfla.be/miv/configuratie/xml)
* xsd of data: [http://miv.opendata.belfla.be/miv-verkeersdata.xsd](http://miv.opendata.belfla.be/miv-verkeersdata.xsd)
* url to retrieve the data: [http://miv.opendata.belfla.be/miv/verkeersdata](http://miv.opendata.belfla.be/miv/verkeersdata)