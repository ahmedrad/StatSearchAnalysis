from initializer import *

top_url_changes_count = sqlContext.read.parquet(get_out_dir() + '/compute__topUrlChangesCount_perKeywordInfo_forAllTimePeriod.parquet')
sorted_data = top_url_changes_count.sort(top_url_changes_count.topUrlChangesCount.desc())

out_file = os.path.join(get_out_dir(), 'topUrlChangesCount_perKeywordInfo_forAllTimePeriod.csv')
sorted_data.repartition(1).write.csv(out_file, mode='overwrite', header=True)
