#!/usr/bin/env bash
set -euo pipefail
cd "$HOME"
hive="${HIVE_HOME:-$HOME/software/apache-hive-3.1.2-bin}/bin/hive"
log="${1:-$HOME/stage7-hive.log}"
{
  date -Is
  printf '%s\n' '>>> hive -e [SHOW TABLES, DESCRIBE FORMATTED and SELECT for each table]'
  sql='USE movielens; SHOW TABLES;'
  for table in ods_ratings dim_users dim_movies dws_movie_stats dws_genre_stats ads_recommendations; do
    sql+=" SELECT '$table' AS inspected_table; DESCRIBE FORMATTED $table; SELECT * FROM $table LIMIT 1;"
  done
  sql+=' SELECT user_id, movie_id, predicted_rating, recommendation_rank FROM ads_recommendations WHERE user_id=1 ORDER BY recommendation_rank LIMIT 10;'
  "$hive" -e "$sql"
  printf '%s\n' HIVE_STAGE7_OK
} 2>&1 | tee "$log"