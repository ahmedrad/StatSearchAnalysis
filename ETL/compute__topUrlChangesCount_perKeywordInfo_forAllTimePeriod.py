from initializer import *

raw_data_file = get_out_dir() + '/crawler_report.csv.gz'
crawler_data_raw = sqlContext.read.csv(raw_data_file, header=True) \
                                  .withColumnRenamed('Crawl Date', 'CrawlDate')
                                  
top_ranked_urls = crawler_data_raw.filter(crawler_data_raw.Rank == 1)
top_ranked_urls.registerTempTable('top_ranked_urls')

top_ranked_urls_changes = sqlContext.sql("""
    SELECT
        Keyword,
        CrawlDate,
        Device,
        Market,
        Location,
        URL AS topUrl,
        LAG(URL) OVER (PARTITION BY Keyword, Device, Market, Location ORDER BY CrawlDate ASC) AS previousTopUrl
    FROM
        top_ranked_urls
""")

top_ranked_urls_changes.registerTempTable('top_ranked_urls_changes')

top_ranked_urls_changes_count = sqlContext.sql("""
    SELECT
        Keyword,
        Device,
        Market,
        Location,
        SUM(CASE WHEN topUrl IS NOT NULL AND previousTopUrl IS NOT NULL AND topUrl != previousTopUrl Then 1 else 0 end) AS topUrlChangesCount
    FROM
        top_ranked_urls_changes
    GROUP BY
        Keyword, Device, Market, Location
    ORDER BY
        topUrlChangesCount DESC
""")

top_ranked_urls_changes_count.write.parquet(get_out_dir() + '/compute__topUrlChangesCount_perKeywordInfo_forAllTimePeriod.parquet', mode='overwrite')
