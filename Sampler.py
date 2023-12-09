import argparse
from fractions import Fraction
import pandas as pd
import numpy as np
import os.path
import sys
from decimal import Decimal
import math 


np.random.seed(6)


def validate_arguments(dataset_file, dataset_size, columns, column_weights):
    file_ext = dataset_file.split('.')[-1]

    if not os.path.isfile(dataset_file) and file_ext == 'csv':
        raise FileNotFoundError('Invalid file path or type')

    df = pd.read_csv(dataset_file)
    # print(df.shape, "shape of df")
    # print(f"{df.head()}")
    if dataset_size > df.shape[0]:
        raise ValueError('Sampling size greater than dataset size')

    for col, weight in zip(columns, column_weights):
        total_w = 0
        if col not in df.columns:
            raise ValueError('Invalid column provided for stratification')
        # print(weight.items())
        for label, w in weight.items():
            # print(type(w))
            if label not in df[col].unique():
                raise ValueError('Invalid label column provided for stratification')
            total_w += w*10
            # print(total_w)
        if total_w != 10:
            raise ValueError('Weights for column stratification are invalid')


def parse_stratification(stratification):
    if stratification == "":
        return [], []

    columns = []
    column_weights = []

    try:
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

            # print(weights)
            columns.append(column)
            column_weights.append(weights)
            # print(column_weights)
    except:
        raise ValueError("Invalid stratification provided")

    return columns, column_weights





def sample_data(dataset_file, dataset_size, columns, column_weights):
    df = pd.read_csv(dataset_file)

    weights = np.ones(df.shape[0])
    # print("column weights ", weights)
    for ind in range(df.shape[0]):
        for col_ind, col in enumerate(columns):
            col_weight = column_weights[col_ind]
            # print(col_weight, "column weight")
            # print(df[col].iloc[ind], "df[col].iloc[ind] ")
            if df[col].iloc[ind] in col_weight:
                weights[ind] *= col_weight[df[col].iloc[ind]]
            else:
                weights[ind] = 0
    
    df["colun_final_weight"] = weights 


    probabilities = weights / weights.sum()
    df["probablities"] = probabilities 
    df.to_csv("wiht_final_probs.csv")

    try:
        sampled_indices = np.random.choice(df.index, size=dataset_size, replace=False, p=probabilities)
        sampled_data = df.loc[sampled_indices]
    except:
        raise ValueError('Given stratification constraints cannot be satisfied for the given dataset')

    return sampled_data


def parse_args(args):
    parser = argparse.ArgumentParser(description="Argument parser for Sampler")
    parser.add_argument("file", type=str, help="csv file containing metadata of all items in data lake")
    parser.add_argument("-s", "--size", type=int, help="size of the dataset after sampling", default=100)
    parser.add_argument("-strat", "--stratification", type=str,
                        help="stratification to be applied for sampling, expected format is <column>: <weightage> <label>, "
                             "<weightage> <label>; <column>: <weightage> <label>, <weightage> <label>", default="")
    parser.add_argument("-o", "--output_file", type=str, help="output file where the sampled data is to be stored in",
                        default="Sample_data.csv")
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    columns, column_weights = parse_stratification(args.stratification)
    # print(columns, column_weights)
    validate_arguments(args.file, args.size, columns, column_weights)
    sampled_data = sample_data(args.file, args.size, columns, column_weights)
    sampled_data.to_csv(args.output_file)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
