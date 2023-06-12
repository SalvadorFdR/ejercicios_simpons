from pyspark.sql import SparkSession
import Simpsons.constants.constants as c
from Simpsons.transform.transformation import Transformation


def main():
    spark = SparkSession.builder.appName(c.APP_NAME).master(c.MODE).getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    t = Transformation()
    # Para la creación del dataframe que lee el archivo CSV:
    simpsons_df = spark.read.option(c.HEADER, c.TRUE_STRING).option(c.DELIMITER, c.VERTICAL_BAR)\
        .csv(c.INPUT_PATH).cache()
    # Para quitar los valores nulos en RATING, VOTES, VIEWERS_IN_MILLIONS
    simpsons_df_1 = t.selec_columns(simpsons_df)
    simpsons_sin_nulos = t.elimina_nulos(simpsons_df_1).cache()
    # Para mostrar el dataframe sin valores nulos:
    print("Para mostrar el dataframe sin valores nulos:")
    simpsons_sin_nulos.show(5)
    # Para mostrar la mejor temporada, basada en el mejor rating:
    print("Para mostrar la mejor temporada, basada en el mejor rating:")
    t.best_season(simpsons_sin_nulos).show(1)
    # Para mostrar el mejor año basado en las Vistas por millon
    print("Para mostrar el mejor año basado en las Vistas por millon")
    t.best_year(simpsons_sin_nulos).show(1)
    # Para mostrar el mejor capitulo:
    print("Para mostrar el mejor capitulo:")
    t.best_chapter(simpsons_sin_nulos).show(1)
    # para mostrar toda la tabla con score
    simpsons_df_score = t.best_chapter_df(simpsons_sin_nulos)
    # Para mostrar el top 3 capitulos:
    print("Para mostrar el top 3 capitulos:")
    best_3_df = t.best_tres(simpsons_df_score)
    final_df = t.select_column(best_3_df)
    final_df.show(600)
    # Para crear el parquet
    print("Para crear el parquet")
    final_df.write.mode(c.MODE_OVERWRITE).parquet(c.OUTPUT)


if __name__ == '__main__':
    main()
