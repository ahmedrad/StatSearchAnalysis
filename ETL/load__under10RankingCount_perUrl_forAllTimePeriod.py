from initializer import *

under_10_ranking_count = sqlContext.read.parquet(get_out_dir() + '/compute__under10RankingCount_perUrl_forAllTimePeriod.parquet')
sorted_data = under_10_ranking_count.sort(under_10_ranking_count.under10RankingCount.desc())

sorted_data.repartition(1).write.csv(get_out_dir() + '/under10RankingCount_perUrl_forAllTimePeriod.csv', mode='overwrite', header=True)
