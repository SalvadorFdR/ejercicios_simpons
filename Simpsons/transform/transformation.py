import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import FloatType, IntegerType
import Simpsons.constants.constants as c
import Simpsons.utils.utils as u


class Transformation:

    def __init__(self):
        pass

    @staticmethod
    def selec_columns(df: DataFrame) -> DataFrame:
        return df.select(*u.diff(df.columns, [c.RATING, c.VOTES, c.VIEWERS_IN_MILLIONS, c.SEASON]),
                         f.col(c.RATING).cast(FloatType()).alias(c.RATING),
                         f.col(c.VOTES).alias(c.VOTES),
                         f.col(c.VIEWERS_IN_MILLIONS).cast(FloatType()).alias(c.VIEWERS_IN_MILLIONS),
                         f.col(c.SEASON).cast("int").alias(c.SEASON))

    @staticmethod
    def elimina_nulos(df: DataFrame) -> DataFrame:
        rating_sin_nulos = f.when(f.col(c.RATING).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.RATING))
        votes_sin_nulos = f.when(f.col(c.VOTES).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.VOTES))
        viewers_in_millions_sin_nulos = f.when(f.col(c.VIEWERS_IN_MILLIONS).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.VIEWERS_IN_MILLIONS))
        return df.select(*u.diff(df.columns, [c.RATING, c.VOTES, c.VIEWERS_IN_MILLIONS]),
                         rating_sin_nulos.alias(c.RATING),
                         votes_sin_nulos.alias(c.VOTES),
                         viewers_in_millions_sin_nulos.alias(c.VIEWERS_IN_MILLIONS))

    @staticmethod
    def best_season(df: DataFrame) -> DataFrame:
        return df.groupby(f.col(c.SEASON)).agg(f.sum(c.RATING).alias(c.TOTAL_RATING))

    @staticmethod
    def best_year(df: DataFrame) -> DataFrame:
        # Utilizando las funciones Window, pero muestra resultados algo extraños
        """window = Window.partitionBy(f.col(c.ORIGINAL_AIR_YEAR)).orderBy(f.col(c.ORIGINAL_AIR_YEAR).desc())
        return df.select(f.col(c.ORIGINAL_AIR_YEAR).alias(c.ORIGINAL_AIR_YEAR),
                         f.sum(f.col(c.VIEWERS_IN_MILLIONS)).over(window).alias(c.VIEWERS_IN_MILLIONS))"""
        # Utilizando función de groupBy y orderBy y muestra los resultados correctos
        return df.groupby(f.col(c.ORIGINAL_AIR_YEAR))\
            .agg(f.sum(f.col(c.VIEWERS_IN_MILLIONS)).alias(c.VIEWERS_IN_MILLIONS))\
            .orderBy(f.col(c.VIEWERS_IN_MILLIONS).desc())

    @staticmethod
    def best_chapter(df: DataFrame) -> DataFrame:
        return df.select(f.col(c.TITLE),
                         (f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS)).alias(c.SCORE))\
                         .orderBy(f.col(c.SCORE).desc())

    @staticmethod
    def best_chapter_df(df: DataFrame) -> DataFrame:
        return df.select(*df.columns,
                         (f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS)).alias(c.SCORE))

    @staticmethod
    def best_tres(df: DataFrame) -> DataFrame:
        """window = Window.partitionBy(f.col(c.SEASON)).orderBy(f.col(c.SCORE).desc())
        return df.select(f.col(c.SEASON),
                         *u.diff(df.columns, [c.IMAGE_URL, c.ID, c.SEASON,
                                              c.NUMBER_IN_SERIES, c.VIDEO_URL]),
                         f.rank().over(window).alias(c.TOP)).agg(f.filter(c.TOP)<=3)"""
        window = Window.partitionBy(c.SEASON).orderBy(f.asc(c.SEASON))
        ranked_df = df.withColumn(c.TOP, f.row_number().over(window))
        filtered_df = ranked_df.filter(f.col(c.TOP) <= 3)
        return filtered_df

    @staticmethod
    def select_column(df: DataFrame) -> DataFrame:
        return df.select(f.col(c.SEASON),
                         f.col(c.TITLE),
                         f.col(c.NUMBER_IN_SEASON),
                         f.col(c.ORIGINAL_AIR_DATE),
                         f.col(c.ORIGINAL_AIR_YEAR),
                         f.col(c.PRODUCTION_CODE),
                         f.col(c.RATING),
                         f.col(c.VOTES),
                         f.col(c.VIEWERS_IN_MILLIONS),
                         f.col(c.SCORE),
                         f.col(c.TOP))



