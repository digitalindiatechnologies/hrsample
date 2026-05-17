import sys
from main.job.pipeline import PySparkJob


def main():
    job = PySparkJob()
    # Load input data to DataFrame
    print("<<Reading CSV>>")
    sample_data_file1_df = job.read_csv(sys.argv[1])
    sample_data_file2_df = job.read_csv(sys.argv[2])

    # Get number of distinct IDs
    print("<<Distinct IDs>>")
    nb_distinct_ids = job.distinct_ids(sample_data_file1_df)
    print(nb_distinct_ids)

    # Get number of valid age
    print("<< Valid age records is greater than equal 18 >>")
    nb_valid_age = job.valid_age_count(sample_data_file2_df)
    print(nb_valid_age)

    job.stop()


if __name__ == '__main__':
    main()
