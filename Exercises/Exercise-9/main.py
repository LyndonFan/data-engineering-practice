from datetime import timedelta
import polars as pl
from pathlib import Path


def cast_types(ldf: pl.LazyFrame) -> pl.LazyFrame:
    ldf = ldf.with_columns(
        [
            pl.col(c).str.to_datetime("%Y-%m-%d %H:%M:%S")
            for c in ["started_at", "ended_at"]
        ]
    )
    return ldf

def augment_ldf(ldf: pl.LazyFrame) -> pl.LazyFrame:
    ldf = ldf.with_columns(
        date=pl.col("started_at").dt.date(),
        week=pl.col("started_at").dt.week(),
    )
    return ldf

def find_daily_bike_rides(ldf: pl.LazyFrame) -> pl.DataFrame:
    return ldf.group_by("date").count().collect()

def find_weekly_rides_metrics(ldf: pl.LazyFrame) -> pl.DataFrame:
    res_ldf = ldf.group_by("week").len()
    res_ldf = res_ldf.select(
        mean_weekly_rides=pl.mean("len"),
        min_weekly_rides=pl.min("len"),
        max_weekly_rides=pl.max("len")
    )
    return res_ldf.collect()

def find_weekly_offset(ldf: pl.LazyFrame) -> pl.DataFrame:
    # need to cast to signed integer
    ldf = ldf.sort("date", descending=False).cast({"count": pl.Int32})
    shifted_ldf = ldf.shift(7).rename({"count": "count_last_week"}).with_columns(
        pl.col("date") + timedelta(days=7)
    )
    ldf = ldf.join(shifted_ldf, on="date", how="left")
    ldf = ldf.with_columns(
        change_from_last_week = pl.col("count") - pl.col("count_last_week")
    ).drop("count_last_week")
    return ldf.collect()


def main(filepath: str | Path):
    ldf = pl.scan_csv(filepath)
    ldf = cast_types(ldf)
    ldf = augment_ldf(ldf)
    daily_bike_rides_df = find_daily_bike_rides(ldf)
    print(daily_bike_rides_df)
    weekly_rides_metrics_df = find_weekly_rides_metrics(ldf)
    print(weekly_rides_metrics_df)
    changes_from_last_week_df = find_weekly_offset(daily_bike_rides_df.lazy())
    print(changes_from_last_week_df)


if __name__ == "__main__":
    filepath = Path("").parent / "data/202306-divvy-tripdata.csv"
    main(filepath)
