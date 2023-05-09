import dask_expr as dd
import numpy as np
import pandas as pd
import pytest

from ..utils_test import downscale_dataframe, run_up_to_nthreads, wait


@pytest.fixture
def taxi_zone_lookup():
    # File from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    df = pd.read_csv(
        "s3://coiled-runtime-ci/xgboost/taxi+_zone_lookup.csv",
        usecols=["LocationID", "Borough"],
    )
    BOROUGH_MAPPING = {
        "Manhattan": "Superborough 1",
        "Bronx": "Superborough 1",
        "EWR": "Superborough 1",
        "Brooklyn": "Superborough 2",
        "Queens": "Superborough 2",
        "Staten Island": "Superborough 3",
        "Unknown": "Unknown",
    }
    df["Superborough"] = [BOROUGH_MAPPING[k] for k in df["Borough"]]
    df = df.astype({"Borough": "string[pyarrow]", "Superborough": "string[pyarrow]"})
    return df


@run_up_to_nthreads("small_cluster", 200, reason="fixed size dataset")
def test_preprocess(small_client, taxi_zone_lookup, read_parquet_with_pyarrow):
    """A typical workflow that preprocesses crude data into a ML-friendly dataframe"""
    ############
    # Read input
    ############
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/xgboost/nyc-uber-lyft-input.parquet",
        index=False,
        columns=[
            "hvfhs_license_num",
            "PULocationID",
            "DOLocationID",
            "trip_miles",
            "trip_time",
            "tolls",
            "congestion_surcharge",
            "airport_fee",
            "wav_request_flag",
            "on_scene_datetime",
            "pickup_datetime",
        ],
    )

    # Full size dataset on 40x r6i.large
    ddf = downscale_dataframe(ddf, "580 GiB")

    # The size of the partitions in the input dataset varies substantially, between 22
    # and 836 MiB. For the purpose of this test we could just rewrite the input in more
    # palatable chunks. However, having to deal with dishomogeneous and unwieldy input
    # partitions is a very realistic use case, which we want to replicate.
    # TODO https://github.com/dask/dask/issues/9849
    # TODO https://github.com/dask/dask/issues/9850
    # This line loads the whole dataset into memory and then discards it.
    ddf = ddf.repartition(partition_size="100MB")

    ####################
    # Preprocess columns
    ####################
    ddf = ddf.assign(
        accessible_vehicle=ddf.on_scene_datetime.isnull(),
        pickup_month=ddf.pickup_datetime.dt.month,
        pickup_dow=ddf.pickup_datetime.dt.dayofweek,
        pickup_hour=ddf.pickup_datetime.dt.hour,
    )
    ddf = ddf.drop(columns=["on_scene_datetime", "pickup_datetime"])
    ddf["airport_fee"] = ddf["airport_fee"].replace("None", 0).astype(float).fillna(0)

    #############
    # Filter rows
    #############
    ddf = ddf.dropna(how="any")
    # Remove outliers
    lower_bound = 0
    Q3 = ddf["trip_time"].quantile(0.75)
    upper_bound = Q3 + (1.5 * (Q3 - lower_bound))
    ddf = ddf.loc[(ddf["trip_time"] >= lower_bound) & (ddf["trip_time"] <= upper_bound)]

    ###################
    # Join with domains
    ###################
    ddf = dd.merge(
        ddf,
        taxi_zone_lookup,
        how="inner",
        left_on="PULocationID",
        right_on="LocationID",
    )
    ddf = ddf.rename(columns={"Borough": "PUBorough", "Superborough": "PUSuperborough"})
    ddf = ddf.drop(columns="LocationID")

    ddf = dd.merge(
        ddf,
        taxi_zone_lookup,
        how="inner",
        left_on="DOLocationID",
        right_on="LocationID",
    )
    ddf = ddf.rename(columns={"Borough": "DOBorough", "Superborough": "DOSuperborough"})
    ddf = ddf.drop(columns="LocationID")

    ddf["PUSuperborough_DOSuperborough"] = ddf.PUSuperborough.str.cat(
        ddf.DOSuperborough, sep="-"
    )
    ddf = ddf.drop(columns=["PUSuperborough", "DOSuperborough"])

    ############
    # Categorize
    ############
    categories = [
        "hvfhs_license_num",
        "PULocationID",
        "DOLocationID",
        "wav_request_flag",
        "accessible_vehicle",
        "pickup_month",
        "pickup_dow",
        "pickup_hour",
        "PUBorough",
        "DOBorough",
        "PUSuperborough_DOSuperborough",
    ]

    # TODO https://github.com/dask/dask/issues/9847
    ddf = ddf.astype(dict.fromkeys(categories, "category"))
    ddf = ddf.persist()
    # This blocks until the whole workload so far has been persisted
    ddf = ddf.categorize(categories)

    ########
    # Output
    ########
    ddf = ddf.persist().repartition(partition_size="100MB")
    # At this point we would normally finish with to_parquet()
    wait(ddf, small_client, timeout=600)


@run_up_to_nthreads("small_cluster", 200, reason="fixed size dataset")
def test_optuna_hpo(small_client):
    xgb = pytest.importorskip("xgboost.dask")
    optuna = pytest.importorskip("optuna")
    mean_squared_error = pytest.importorskip("dask_ml.metrics").mean_squared_error

    #############################
    # Dataset load and preprocess
    #############################
    # Load feature table generated by previous test
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/xgboost/nyc-uber-lyft-preprocessed.parquet",
    )

    # Full size dataset on 40x r6i.large
    ddf = downscale_dataframe(ddf, "580 GiB")

    # Under the hood, XGBoost converts floats to `float32`.
    # Let's do it only once here.
    float_cols = ddf.select_dtypes(include="float").columns.tolist()
    ddf = ddf.astype({c: np.float32 for c in float_cols})

    # We need the categories to be known
    categorical_vars = ddf.select_dtypes(include="category").columns.tolist()

    # categorize() reads the whole input and then discards it.
    # Let's read from disk only once.
    ddf = ddf.persist()
    ddf = ddf.categorize(columns=categorical_vars)

    ####################################
    # Split between train and test data
    # and initialize the XGBoost DMatrix
    ####################################
    train, test = ddf.random_split([0.8, 0.2], shuffle=True)
    x_train = train.drop(columns=["trip_time"])
    y_train = train["trip_time"]
    x_test = test.drop(columns="trip_time")
    y_test = test["trip_time"]

    # We will need to access these multiple times. Let's persist them.
    x_test, y_test = small_client.persist([x_test, y_test])

    # Release no longer necessary objects on the cluster
    del ddf, train, test

    # Build XGBoost matrix. This is automatically persisted.
    d_train = xgb.DaskDMatrix(None, x_train, y_train, enable_categorical=True)

    del x_train, y_train

    #########################################
    # Have Optuna call xgboost.train multiple
    # times with different hyperparameters
    #########################################
    def train_model(**study_params):
        model = xgb.train(
            None,
            {"tree_method": "hist", **study_params},
            d_train,
            num_boost_round=4,
            evals=[(d_train, "train")],
        )
        predictions = xgb.predict(None, model, x_test)
        return mean_squared_error(
            y_test.to_dask_array(),
            predictions.to_dask_array(),
            squared=False,
        )

    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 75, 125),
            "learning_rate": trial.suggest_float("learning_rate", 0.5, 0.7),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1),
            "colsample_bynode": trial.suggest_float("colsample_bynode", 0.5, 1),
            "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.5, 1),
            "reg_lambda": trial.suggest_float("reg_lambda", 0, 1),
            "max_depth": trial.suggest_int("max_depth", 1, 6),
            "max_leaves": trial.suggest_int("max_leaves", 0, 2),
            "max_cat_to_onehot": trial.suggest_int("max_cat_to_onehot", 1, 10),
        }
        return train_model(**params)

    study = optuna.create_study(study_name="nyc-travel-time-model")
    study.optimize(objective, n_trials=3)
    # This will vary between 400 and 800. To get a stable number we'd need the full
    # dataset and a number of trials much higher than 3.
    assert 100 < study.best_value < 1500
