
To upload components:

`az ml component create --file .\clarityCleanupComponent.yaml --resource-group rg-fecdata --workspace fecaml`

`az ml component create --file .\insulinSheetCleanup.yml --resource-group rg-fecdata --workspace fecaml --version 1`

To upload the job:

`az ml job create --file .\glucoseTimeSeriesPipeline.yaml --resource-group rg-fecdata --workspace fecaml`

Env:

`az ml environment create --file ..\assets\environments\timeSeriesModelingEnv.yaml --resource-group rg-fecdata --workspace fecaml`