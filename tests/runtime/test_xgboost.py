import dask.dataframe as dd
import pytest

dask_ml = pytest.importorskip("dask_ml")
dxgb = pytest.importorskip("xgboost.dask")


def test_xgboost_distributed_training(small_client):
    ddf = dd.read_parquet(
        "s3://coiled-datasets/synthetic-data/synth-reg-104GB.parquet",
        storage_options={"anon": True},
    )
    ddf = ddf.partitions[0:30]
    ddf = ddf.persist()

    # Create the train-test split
    X, y = ddf.iloc[:, :-1], ddf["target"]
    X_train, X_test, y_train, y_test = dask_ml.model_selection.train_test_split(
        X, y, test_size=0.3, shuffle=True, random_state=21
    )

    # Create the XGBoost DMatrix for our training and testing splits
    dtrain = dxgb.DaskDMatrix(small_client, X_train, y_train)
    dtest = dxgb.DaskDMatrix(small_client, X_test, y_test)

    # Set model parameters (XGBoost defaults)
    params = {
        "max_depth": 6,
        "gamma": 0,
        "eta": 0.3,
        "min_child_weight": 30,
        "objective": "reg:squarederror",
        "grow_policy": "depthwise",
    }
    output = dxgb.train(
        small_client, params, dtrain, num_boost_round=5, evals=[(dtrain, "train")]
    )

    # make predictions
    y_pred = dxgb.predict(small_client, output, dtest)
    assert y_pred.shape[0] == y_test.shape[0].compute()
