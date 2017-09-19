from initializer import *
from pyspark.sql.functions import abs, mean, col, stddev

raw_data_file = get_out_dir() + '/crawler_report.csv.gz'
crawler_data_raw = sqlContext.read.csv(raw_data_file, header=True) \
                             .withColumnRenamed('Crawl Date', 'CrawlDate')

desktop_ranks = crawler_data_raw.filter(crawler_data_raw.Device == 'desktop') \
                                .withColumnRenamed('Rank', 'desktopRank')
smartphone_ranks = crawler_data_raw.filter(crawler_data_raw.Device == 'smartphone') \
                                   .withColumnRenamed('Rank', 'smartphoneRank')

join_condition = [
    desktop_ranks.CrawlDate == smartphone_ranks.CrawlDate,
    desktop_ranks.Keyword == smartphone_ranks.Keyword,
    desktop_ranks.Market == smartphone_ranks.Market,
    desktop_ranks.URL == smartphone_ranks.URL,
    (desktop_ranks.Location == smartphone_ranks.Location) | (desktop_ranks.Location.isNull() & smartphone_ranks.Location.isNull())
]

joined_data = desktop_ranks.join(smartphone_ranks, join_condition)
joined_data = joined_data.select([desktop_ranks[x] for x in desktop_ranks.columns] + ['smartphoneRank']).drop('Device')

joined_data = joined_data.withColumn('rankDifference', abs(joined_data.desktopRank - joined_data.smartphoneRank))

grouped_data = joined_data.groupBy('CrawlDate').agg(mean(col('rankDifference')), stddev(col('rankDifference')))
grouped_data = grouped_data.withColumnRenamed('avg(rankDifference)', 'meanDeviceRankingDifference')
grouped_data = grouped_data.withColumnRenamed('stddev_samp(rankDifference)', 'stddevDeviceRankingDifference')
grouped_data.write.parquet(get_out_dir() + '/compute__deviceRankingDifference_perDay.parquet', mode='overwrite')
