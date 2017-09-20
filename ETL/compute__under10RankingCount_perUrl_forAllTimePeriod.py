from initializer import *

raw_data_file = os.path.join(get_out_dir(), 'crawler_report.csv.gz')
crawler_data_raw = sqlContext.read.csv(raw_data_file, header=True) \
                                  .withColumnRenamed('Crawl Date', 'CrawlDate')

under_10_rankings = crawler_data_raw.filter(crawler_data_raw.Rank < 10) \
                                            .groupBy(crawler_data_raw.URL) \
                                            .agg({'*': 'count'})

under_10_rankings = under_10_rankings.withColumnRenamed('count(1)', 'under10RankingCount')

out_file = os.path.join(get_out_dir(), 'compute__under10RankingCount_perUrl_forAllTimePeriod.parquet')
under_10_rankings.write.parquet(out_file, mode='overwrite')
