import argparse
import pandas as pd
import numpy as np
import os.path
import sys
import math 


np.random.seed(6)
# to make the results reproducible 


def validate_arguments(dataset_file, dataset_size, columns, column_weights):
    file_ext = dataset_file.split('.')[-1]

    if not os.path.isfile(dataset_file) and file_ext == 'csv':
        raise FileNotFoundError('Invalid file path or type')

    df = pd.read_csv(dataset_file)

    if dataset_size > df.shape[0]:
        raise ValueError('Sampling size greater than datalake size')

    for col, weight in zip(columns, column_weights):
        total_w = 0
        if col not in df.columns:
            raise ValueError('Invalid column provided for stratification')

        for label, w in weight.items():
            if label not in df[col].unique():
                raise ValueError('Invalid columnar value')
            total_w += w

        if not math.isclose(total_w, 1.0):
            raise ValueError('Invalid weights - weights should sum up to 1.0)')


def parse_stratification(stratification):
    if stratification == "":
        return [], []

    columns = []
    column_weights = []

    
    semicolon_split = stratification.strip().split(';')
    for semi_split in semicolon_split:
        colon_split = semi_split.strip().split(':')
        column = colon_split[0].strip()
        weights_split = colon_split[1].strip().split(',')
        weights = {}
        for sp in weights_split:
            label_split = sp.strip().split(' ')
            weight = float(label_split[0])
            label = label_split[1]
            weights[label] = weight

        columns.append(column)
        column_weights.append(weights)

    return columns, column_weights


def sample_data(dataset_file, dataset_size, columns, column_weights):
    df = pd.read_csv(dataset_file)
    sampled_data = pd.DataFrame(columns=df.columns)

    for col_ind, col in enumerate(columns):
        col_weight = column_weights[col_ind]
        for label, weight in col_weight.items():
            label_data = df[df[col] == label].sample(n=int(dataset_size * weight), replace=True)
            sampled_data = pd.concat([sampled_data, label_data])

    # sampled_data.to_csv("full.csv")

    try:
        sampled_data = sampled_data.head(dataset_size)
    except:
        raise ValueError('Given constraints cannot be met for the given dataset')

    return sampled_data


def parse_args(args):
    parser = argparse.ArgumentParser(description="Argument parser for Sampler")
    parser.add_argument("file", type=str, help="Data Lake")
    parser.add_argument("-s", "--size", type=int, help="size of the dataset after sampling", default=100)
    parser.add_argument("-strat", "--stratification", type=str,
                        help="stratification to be applied for sampling, expected format is <column>: <weightage> <label>, "
                             "<weightage> <label>; <column>: <weightage> <label>, <weightage> <label>", default="")
    parser.add_argument("-o", "--output_file", type=str, help="output file where the sampled data is to be stored in",
                        default="sampled_data.csv")
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    columns, column_weights = parse_stratification(args.stratification)
    validate_arguments(args.file, args.size, columns, column_weights)
    sampled_data = sample_data(args.file, args.size, columns, column_weights)
    sampled_data.to_csv(args.output_file, index=False)
    

if __name__ == "__main__":
    main(sys.argv[1:])

