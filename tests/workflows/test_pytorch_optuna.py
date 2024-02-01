# Derived partially from https://pytorch.org/tutorials/beginner/dcgan_faces_tutorial.html
import os
import pathlib
import tempfile
import zipfile

import pytest
from dask.distributed import Lock, PipInstall, get_worker

from ..utils_test import wait

pytestmark = pytest.mark.workflows

optuna = pytest.importorskip("optuna")


@pytest.mark.xfail(
    raises=TimeoutError,
    reason=(
        "Occasionally fails to install/attach CUDA, "
        "will then run on CPU and get a TimeoutError"
    ),
)
@pytest.mark.client(
    "pytorch_optuna",
    worker_plugin=PipInstall(
        [
            # FIXME https://github.com/coiled/platform/issues/1249
            #       package_sync doesn't seem to deduce GPU specific installs of
            #       libraries like torch
            "torch",
            # FIXME Windows package_sync doesn't like torchvision
            "torchvision",
        ],
        pip_options=[
            "--index-url",
            "https://download.pytorch.org/whl/cu118",
            "--extra-index-url",
            "https://pypi.org/simple",
        ],
        restart_workers=True,
    ),
)
def test_hpo(client):
    """Run example of running PyTorch and Optuna together on Dask"""
    NC = 3  # Number of channels in the training images. For color images this is 3
    NZ = 100  # Size of z latent vector (i.e. size of generator input)

    def weights_init(m):
        import torch.nn as nn

        if isinstance(m, (nn.ConvTranspose2d, nn.Conv2d)):
            nn.init.normal_(m.weight.data, 0.0, 0.02)
        elif isinstance(m, nn.BatchNorm2d):
            nn.init.normal_(m.weight.data, 1.0, 0.02)
            nn.init.constant_(m.bias.data, 0)

    def download_data():
        import s3fs  # FIXME: see above install w/ urllib3 - import here after reinstall

        worker = get_worker()
        with Lock(worker.address):
            tmpdir = tempfile.gettempdir()
            zip = pathlib.Path(tmpdir).joinpath("img_align_celeba.zip")
            dataset_dir = zip.parent.joinpath("img_align_celeba")

            if zip.exists():
                print("Dataset already downloaded, returning dataset dir")
                return dataset_dir

            print("Downloading dataset...")
            fs = s3fs.S3FileSystem(anon=True)
            fs.download(
                "s3://coiled-datasets/CelebA-Faces/img_align_celeba.zip", str(zip)
            )

            print(f"Unzipping into {dataset_dir}")
            with zipfile.ZipFile(str(zip), "r") as zipped:
                zipped.extractall(dataset_dir)
            return dataset_dir

    def get_generator(trial):
        import torch.nn as nn

        # Create and randomly initialize generator model
        activations = ["Sigmoid", "Softmax"]
        activation = trial.suggest_categorical("generator_activation", activations)
        bias = bool(trial.suggest_int("generator_bias", 0, 1))
        ngf = 64  # Size of feature maps in generator
        model = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d(NZ, ngf * 8, 4, 1, 0, bias=bias),
            nn.BatchNorm2d(ngf * 8),
            nn.ReLU(True),
            # state size. ``(ngf*8) x 4 x 4``
            nn.ConvTranspose2d(ngf * 8, ngf * 4, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ngf * 4),
            nn.ReLU(True),
            # state size. ``(ngf*4) x 8 x 8``
            nn.ConvTranspose2d(ngf * 4, ngf * 2, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ngf * 2),
            nn.ReLU(True),
            # state size. ``(ngf*2) x 16 x 16``
            nn.ConvTranspose2d(ngf * 2, ngf, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ngf),
            nn.ReLU(True),
            # state size. ``(ngf) x 32 x 32``
            nn.ConvTranspose2d(ngf, NC, 4, 2, 1, bias=bias),
            getattr(nn, activation)(),
            # state size. ``(NC) x 64 x 64``
        )
        model.apply(weights_init)
        return model

    def get_discriminator(trial):
        import torch.nn as nn

        # Create and randomly initialize discriminator model
        activations = ["Sigmoid", "Softmax"]
        activation = trial.suggest_categorical("discriminator_activation", activations)
        bias = bool(trial.suggest_int("discriminator_bias", 0, 1))
        dropout = trial.suggest_float("discriminator_dropout", 0, 0.3)
        leaky_relu_slope = trial.suggest_float(
            "discriminator_leaky_relu_slope", 0.1, 0.4, step=0.1
        )
        ndf = 64  # Size of feature maps in discriminator
        model = nn.Sequential(
            # input is ``(NC) x 64 x 64``
            nn.Conv2d(NC, ndf, 4, 2, 1, bias=bias),
            nn.LeakyReLU(leaky_relu_slope, inplace=True),
            nn.Dropout2d(p=dropout),
            # state size. ``(ndf) x 32 x 32``
            nn.Conv2d(ndf, ndf * 2, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ndf * 2),
            nn.LeakyReLU(leaky_relu_slope, inplace=True),
            # state size. ``(ndf*2) x 16 x 16``
            nn.Conv2d(ndf * 2, ndf * 4, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ndf * 4),
            nn.LeakyReLU(leaky_relu_slope, inplace=True),
            # state size. ``(ndf*4) x 8 x 8``
            nn.Conv2d(ndf * 4, ndf * 8, 4, 2, 1, bias=bias),
            nn.BatchNorm2d(ndf * 8),
            nn.LeakyReLU(leaky_relu_slope, inplace=True),
            # state size. ``(ndf*8) x 4 x 4``
            nn.Conv2d(ndf * 8, 1, 4, 1, 0, bias=bias),
            getattr(nn, activation)(),
        )
        model.apply(weights_init)
        return model

    def objective(trial):
        import torch
        import torch.nn as nn
        import torch.optim
        import torch.utils.data
        import torchvision.datasets as dset
        import torchvision.transforms as transforms

        dataset_dir = download_data()

        dataset = dset.ImageFolder(
            root=str(dataset_dir),
            transform=transforms.Compose(
                [
                    transforms.Resize(64),
                    transforms.CenterCrop(64),
                    transforms.ToTensor(),
                    transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5)),
                ]
            ),
        )

        # Datset is just over 200k, for this example we'll use the first 10k
        subset = torch.utils.data.Subset(dataset, torch.arange(10_000))
        dataloader = torch.utils.data.DataLoader(
            subset, batch_size=128, shuffle=True, num_workers=0
        )

        if not torch.cuda.is_available() and os.environ.get("CI"):
            raise RuntimeError("Expected to run on GPU but it's not available!")
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        # Create models
        generator = get_generator(trial).to(device)
        discriminator = get_discriminator(trial).to(device)

        # Establish convention for real and fake labels during training
        real_label = 1.0
        fake_label = 0.0

        # Setup loss criterion / optimizers and train
        loss = nn.BCELoss()
        optimizer_discriminator = torch.optim.Adam(
            discriminator.parameters(), lr=0.00001, betas=(0.5, 0.999)
        )
        optimizer_generator = torch.optim.Adam(
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

    n_trials = 50
    futures = [
        client.submit(study.optimize, objective, n_trials=1, pure=False)
        for _ in range(n_trials)
    ]
    wait(futures, client, 900)

    assert len(study.trials) >= n_trials
