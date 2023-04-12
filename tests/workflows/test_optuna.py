import uuid

import coiled
import optuna
import pytest
import xgboost as xgb
from dask.distributed import Client, wait
from optuna.integration.dask import DaskStorage
from optuna.samplers import RandomSampler
from sklearn.datasets import fetch_covtype
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import LabelEncoder


@pytest.fixture(scope="module")
def optuna_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    with coiled.Cluster(
        f"optuna-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["optuna_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def optuna_client(
    optuna_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["optuna_cluster"]["n_workers"]
    with Client(optuna_cluster) as client:
        optuna_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_optuna(optuna_client):
    # We use a random sampler with a seed to get deterministic results.
    # This is just for benchmarking purposes.
    study = optuna.create_study(
        direction="maximize", storage=DaskStorage(), sampler=RandomSampler(seed=2)
    )

    def objective(trial):
        # Dataset (241.59 MiB) from http://archive.ics.uci.edu/ml/datasets/covertype
        X, y = fetch_covtype(return_X_y=True)

        # Format training labels
        le = LabelEncoder()
        y = le.fit_transform(y)

        # Get hyperparameter values for this trial and model score
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 2, 10),
            "max_depth": trial.suggest_int("max_depth", 2, 10),
            "learning_rate": trial.suggest_float("learning_rate", 1e-8, 1.0, log=True),
            "subsample": trial.suggest_float("subsample", 0.2, 1.0),
            "eval_metric": "mlogloss",
            "n_jobs": 1,  # Avoid thread oversubscription
        }
        model = xgb.XGBClassifier(**params)
        cv = KFold(n_splits=3, shuffle=True, random_state=2)
        score = cross_val_score(model, X, y, cv=cv, scoring="accuracy")
        return score.mean()

    # Run HPO trials on a cluster
    n_trials = 200
    futures = [
        optuna_client.submit(study.optimize, objective, n_trials=1, pure=False)
        for _ in range(n_trials)
    ]
    wait(futures)
    assert len(study.trials) == n_trials
