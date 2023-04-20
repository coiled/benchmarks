# Derived partially from
# - https://pytorch.org/tutorials/beginner/dcgan_faces_tutorial.html
import uuid

import coiled
import numpy as np
import optuna
import pytest
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
from dask.distributed import Client, PipInstall, wait

NC = 3  # Number of channels in the training images. For color images this is 3
NZ = 100  # Size of z latent vector (i.e. size of generator input)


@pytest.fixture(scope="module")
def pytorch_optuna_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    with coiled.Cluster(
        name=f"pytorch-optuna-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["pytorch_optuna_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def pytorch_optuna_client(
    pytorch_optuna_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["pytorch_optuna_cluster"]["n_workers"]
    with Client(pytorch_optuna_cluster) as client:
        pytorch_optuna_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.upload_file(__file__)
        # sync_packages won't work b/c local env won't have Cuda/GPU stuff
        # and using only software environments won't pick up local project.
        # Therefore, we'll install the stuff we need at runtime.
        plugin = PipInstall(
            [
                "torch==2.0.0",
                "git+https://github.com/optuna/optuna.git@378508ab6bddbad182bdfa0e8b3ad4bbb7040f00",
            ],
            pip_options=["--force-reinstall"],
            restart=True,
        )
        client.register_worker_plugin(plugin)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


# We create a fake image dataset. This ought to be replaced by
# your actual dataset or pytorch's example datasets. We do this here
# to focus more on computation than actual convergence.
class FakeImageDataset(torch.utils.data.Dataset):
    def __init__(self, count=1_000):
        self.labels = np.random.randint(0, 5, size=count)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        label = self.labels[idx]
        torch.random.seed = label
        # Spatial size of training images. All images will be resized to this
        # size using a transformer.
        img = torch.rand(3, 64, 64)
        return img, label


def weights_init(m):
    classname = m.__class__.__name__
    if classname.find("Conv") != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find("BatchNorm") != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


def get_generator(trial):
    # Create and randomly initialize generator model
    activations = ["Sigmoid"]
    activation = trial.suggest_categorical("generator_activation", activations)
    ngf = 64  # Size of feature maps in generator
    model = nn.Sequential(
        # input is Z, going into a convolution
        nn.ConvTranspose2d(NZ, ngf * 8, 4, 1, 0, bias=False),
        nn.BatchNorm2d(ngf * 8),
        nn.ReLU(True),
        # state size. ``(ngf*8) x 4 x 4``
        nn.ConvTranspose2d(ngf * 8, ngf * 4, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ngf * 4),
        nn.ReLU(True),
        # state size. ``(ngf*4) x 8 x 8``
        nn.ConvTranspose2d(ngf * 4, ngf * 2, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ngf * 2),
        nn.ReLU(True),
        # state size. ``(ngf*2) x 16 x 16``
        nn.ConvTranspose2d(ngf * 2, ngf, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ngf),
        nn.ReLU(True),
        # state size. ``(ngf) x 32 x 32``
        nn.ConvTranspose2d(ngf, NC, 4, 2, 1, bias=False),
        getattr(nn, activation)(),
        # state size. ``(NC) x 64 x 64``
    )
    model.apply(weights_init)
    return model


def get_discriminator(trial):
    # Create and randomly initialize discriminator model
    activations = ["Sigmoid"]
    activation = trial.suggest_categorical("discriminator_activation", activations)
    leaky_relu_slope = trial.suggest_float(
        "discriminator_leaky_relu_slope", 0.1, 0.4, step=0.1
    )
    ndf = 64  # Size of feature maps in discriminator
    model = nn.Sequential(
        # input is ``(NC) x 64 x 64``
        nn.Conv2d(NC, ndf, 4, 2, 1, bias=False),
        nn.LeakyReLU(leaky_relu_slope, inplace=True),
        # state size. ``(ndf) x 32 x 32``
        nn.Conv2d(ndf, ndf * 2, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ndf * 2),
        nn.LeakyReLU(leaky_relu_slope, inplace=True),
        # state size. ``(ndf*2) x 16 x 16``
        nn.Conv2d(ndf * 2, ndf * 4, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ndf * 4),
        nn.LeakyReLU(leaky_relu_slope, inplace=True),
        # state size. ``(ndf*4) x 8 x 8``
        nn.Conv2d(ndf * 4, ndf * 8, 4, 2, 1, bias=False),
        nn.BatchNorm2d(ndf * 8),
        nn.LeakyReLU(leaky_relu_slope, inplace=True),
        # state size. ``(ndf*8) x 4 x 4``
        nn.Conv2d(ndf * 8, 1, 4, 1, 0, bias=False),
        getattr(nn, activation)(),
    )
    model.apply(weights_init)
    return model


def test_hpo(pytorch_optuna_client):
    """Run example of running PyTorch and Optuna together on Dask"""

    def objective(trial):
        dataset = FakeImageDataset()

        # Create the dataloader
        # NOTE: IMPORTANT w/ optuna; it launches a daemonic process, so PyTorch can't itself use it then.
        dataloader = torch.utils.data.DataLoader(
            dataset, batch_size=64, shuffle=True, num_workers=0
        )
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        # Create models
        generator = get_generator(trial).to(device)
        discriminator = get_discriminator(trial).to(device)

        # Establish convention for real and fake labels during training
        real_label = 1.0
        fake_label = 0.0

        # Setup loss criterion / optimizers and train
        loss = nn.BCELoss()
        optimizer_discriminator = optim.Adam(
            discriminator.parameters(), lr=0.00001, betas=(0.5, 0.999)
        )
        optimizer_generator = optim.Adam(
            generator.parameters(), lr=0.00001, betas=(0.5, 0.999)
        )
        num_epochs = 5
        for epoch in range(num_epochs):
            # For each batch in the dataloader
            for batch in dataloader:
                # Train discriminator with all-real batch (maximize log(D(x)) + log(1 - D(G(z))))
                discriminator.zero_grad()
                # Format data batch
                real_cpu = batch[0].to(device)
                batch_size = real_cpu.size(0)
                label = torch.full([batch_size], real_label, device=device)
                # Forward pass real batch through D
                output = discriminator(real_cpu).view(-1)
                loss_real = loss(output, label)
                loss_real.backward()

                # Train with all-fake batch
                # Generate fake image batch with G
                noise = torch.randn(batch_size, NZ, 1, 1, device=device)
                fake = generator(noise)
                label.fill_(fake_label)

                # Classify all fake batch with D
                output = discriminator(fake.detach()).view(-1)
                loss_fake = loss(output, label)
                loss_fake.backward()

                # Compute error of discriminator as sum over the fake and the real batches
                loss_discriminator = loss_real + loss_fake
                optimizer_discriminator.step()

                # Update generator: maximize log(D(G(z)))
                generator.zero_grad()
                # fake labels are real for generator cost
                label.fill_(real_label)
                # Since we just updated D, perform another forward pass of all-fake batch through D
                output = discriminator(fake).view(-1)
                loss_generator = loss(output, label)
                loss_generator.backward()

                # Update G
                optimizer_generator.step()

            # Handle pruning based on the intermediate value
            score = loss_discriminator.item()
            trial.report(score, epoch)
            if trial.should_prune():
                raise optuna.exceptions.TrialPruned()

        return score

    study = optuna.create_study(
        storage=optuna.integration.DaskStorage(), direction="minimize"
    )

    n_trials = 500
    futures = [
        pytorch_optuna_client.submit(study.optimize, objective, n_trials=1, pure=False)
        for _ in range(n_trials)
    ]
    wait(futures)

    assert len(study.trials) == n_trials
