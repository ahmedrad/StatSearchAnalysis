from initializer import *

device_ranking_difference = sqlContext.read.parquet(get_out_dir() + '/compute__deviceRankingDifference_perDay.parquet')
sorted_data = device_ranking_difference.sort(device_ranking_difference.CrawlDate.desc())

sorted_data.repartition(1).write.csv(get_out_dir() + '/deviceRankingDifference_perDay.csv', mode='overwrite', header=True)
