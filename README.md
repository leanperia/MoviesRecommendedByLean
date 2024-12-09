# MoviesRecommendedByLean

## IMDb datasets Case Study on AAA movies

This is my project on a case study for predicting AAA movies based on data from the IMDb Non-commercial datasets.

I added the following datasets: 
- [MovieLens 32M 2023 dataset](https://grouplens.org/datasets/movielens/)
- [TMDB Movies 1M dataset](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies) from Kaggle. 

Below is the full process:
1. Download the datasets as specified in the first section of `download_and_explore_data_v2.ipynb`
2. Run `ingest_imdb.py`, `profile_datasets.py` and then `ingest_other_datasets.py`
3. Continue running the code in `download_and_explore_data_v2.ipynb`
4. Run `predict_aaa_movies_v2.ipynb`

The notebook `webscraping.ipynb` is provided as an attempt to obtain box office data from Wikidata. However I found out it is insufficient for my purposes. 


## Movie Recommender on MovieLens dataset
I wrote the notebook `movie_recommender.ipynb` training a recommender on the MovieLens 20M dataset.
I chose K-nearest neighbors and Non-negative Matrix Factorization.

## Libraries necessary

```
pandas
scikit-learn
duckdb
ydata-profiling
xgboost
shap
matplotlib
```