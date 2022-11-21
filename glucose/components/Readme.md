
To upload components:

`az ml component create --file .\clarityCleanupComponent.yaml --resource-group rg-fecdata --workspace fecaml`

To upload the job:

`az ml job create --file .\glucoseTimeSeriesPipeline.yaml --resource-group rg-fecdata --workspace fecaml`

Env:

`az ml environment create --file ..\assets\environments\timeSeriesModelingEnv.yaml --resource-group rg-fecdata --workspace fecaml`