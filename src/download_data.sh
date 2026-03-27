#!/bin/bash

# Make sure the data folder exists
mkdir -p ../data

# Download the dataset ZIP into the data folder
kaggle datasets download rushdaismailaslami/goodreads-sampled-dataset-for-nlp-ucsd-derived -p ../data

# Unzip the dataset into the data folder
unzip -o ../data/goodreads-sampled-dataset-for-nlp-ucsd-derived.zip -d ../data

# remove the ZIP to save space
rm ../data/goodreads-sampled-dataset-for-nlp-ucsd-derived.zip