To launch pipelines:

`az ml environment create --file .\fecPipelineEnv.yml --resource-group rg-fecdata --workspace fecaml`



Project notes:
what do you want to do with this data? 
1) better open secrets - map from candidate to committees to smart summaries:
    by zip (on a map)
    by overlap? what else does each person donate to? 

2) consumer brands/companies and their donations. suggest alternative in same industry? needs a set of industry/company maps.

3) multiple maxouts in small companies? look for those. see if you can get a sense for size of company. lol find a scandal.

4) multiple candidates vs single candidate - who donates to more than one person? this is sort of wild idea.
    how to handle people giving to committee? I guess same way.

5) pull together zip stats from census. try to predict level of and direction of donation. find outliers and predictors. 
    very likely outliers high will have more govt contracts nearby...
    can you get all federal contracts??


Need to remember I linked the data account in synapse and then selected "get one file" and now i have all of them.
