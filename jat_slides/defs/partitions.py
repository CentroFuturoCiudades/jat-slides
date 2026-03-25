import dagster as dg

# with PostGISResource.connect() as conn:
#     df_mun = pd.read_sql(
#         """
#         SELECT "CVEGEO" FROM census_2020_mun
#         """,
#         conn
#     )
# mun_list = df_mun["CVEGEO"].sort_values().astype(str).to_numpy().tolist()
# mun_partitions = StaticPartitionsDefinition(mun_list)

mun_partitions = dg.StaticPartitionsDefinition(["01001"])
