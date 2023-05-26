from autogen.datagen import DataGenerator





def main(args):

    ## Inputs:
    ## CONNECTION_NAME (depends on CML DataLake)
    ## ARGS FOR GENERATE_DF METHOD: PARTITIONS_NUM, ROW_COUNT, UNIQUE_VALS, DISPLAY_OPION
    ## TABLE NAME

    import cml.data_v1 as cmldata

    conn = cmldata.get_connection(CONNECTION_NAME)
    spark = conn.get_spark_session()



    df = DataGenerator.generate_df(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)
    DataGenerator.save_table(df, table_name_prefix)

if __name__ == '__main__':
    main()
