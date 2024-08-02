import pytest
from dask.distributed import wait

pytestmark = pytest.mark.workflows

optuna = pytest.importorskip("optuna")
xgb = pytest.importorskip("xgboost")
pytest.importorskip("sklearn")

from optuna.samplers import RandomSampler  # noqa: E402
from optuna_integration import DaskStorage  # noqa: E402
from sklearn.datasets import fetch_covtype  # noqa: E402
from sklearn.model_selection import KFold, cross_val_score  # noqa: E402
from sklearn.preprocessing import LabelEncoder  # noqa: E402


@pytest.mark.client("xgboost_optuna")
def test_hpo(client):
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
            "n_jobs": 1,  # Avoid thread oversubscription
        }
        model = xgb.XGBClassifier(**params)
        cv = KFold(n_splits=3, shuffle=True, random_state=2)
        score = cross_val_score(model, X, y, cv=cv)
        return score.mean()

    # Run HPO trials on a cluster
    n_trials = 200
    futures = [
        client.submit(study.optimize, objective, n_trials=1, pure=False)
        for _ in range(n_trials)
    ]
    wait(futures)
    assert len(study.trials) >= n_trials
