FilingAlias = Filing.alias()

#Subquery to get max filing per adviser.

subquery = (FilingAlias.select(
                FilingAlias.adviser,
                fn.MAX(FilingAlias.id).alias('most_recent'))
            .group_by(FilingAlias.adviser)
            .alias('most_recent_subquery'))

# Query for filings and join using the subquery
    #to match the filings's adviser and id.

most_recent_filings = (
    Filing.select(Filing.id.alias("filing_id"), Adviser.crd)
         .join(Adviser)
         .switch(Filing)
         .join(subquery, on=(
            (Filing.id == subquery.c.most_recent) &
            (Filing.adviser == subquery.c.adviser_id)))
                )

#this will be the index table

#get all most recent items per adviser

#dict of dicts / category --> data
#...

#get all other items per adviser

#dict of dicts / category --> data
#...

