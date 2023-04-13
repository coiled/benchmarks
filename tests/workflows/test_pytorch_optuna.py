# Derived partially from
# - https://pytorch.org/tutorials/beginner/dcgan_faces_tutorial.html
import uuid

import optuna
from optuna.integration.dask import DaskStorage

import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
import torchvision.utils as vutils
import numpy as np

import coiled
import pytest
from dask.distributed import Client
from dask.distributed import wait

# Number of training epochs
num_epochs = 20

# Batch size during training
batch_size = 128

# Spatial size of training images. All images will be resized to this
#   size using a transformer.
image_size = 64

# Number of channels in the training images. For color images this is 3
nc = 3

# Size of z latent vector (i.e. size of generator input)
nz = 100

# Size of feature maps in generator
ngf = 64

# Size of feature maps in discriminator
ndf = 64

# Beta1 hyperparameter for Adam optimizers
beta1 = 0.5

# Number of GPUs available. Use 0 for CPU mode.
ngpu = 1


@pytest.fixture(scope="module")
def pytorch_optuna_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    name = f"pytorch-optuna-{uuid.uuid4().hex[:8]}"

    # coiled.create_software_environment(
    #     name=name,
    #     conda={
    #         "channels": ["pytorch", "nvidia", "conda-forge", "defaults"],
    #         "dependencies": [
    #             "dask=2023.2",
    #             "pytorch=2.0.0",
    #             "pytorch-cuda=11.8",
    #             "optuna=3.1.0",
    #             "torchvision",
    #             "cudatoolkit=11.8.0",
    #             "pynvml=11.4.1",
    #         ],
    #     },
    #     pip=[
    #         "git+https://github.com/coiled/benchmarks.git@pytorch-optuna-workflow",
    #         "pytest"
    #     ],
    #     gpu_enabled=True,
    # )

    # Remove auto default 'package_sync', error having both
    # package_sync and software specified
    kwargs = cluster_kwargs["pytorch_optuna_cluster"]
    # kwargs.pop("package_sync", True)
    kwargs['package_sync'] = True

    with coiled.Cluster(
        name=name,
        software=name,
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **kwargs,
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
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_pytorch_optuna_hyper_parameter_optimization(pytorch_optuna_client):
    """Run example of running PyTorch and Optuna together on Dask"""

    dataset = FakeImageDataset()

    # Create the dataloader
    dataloader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, shuffle=True, num_workers=2
    )

    # Decide which device we want to run on
    device = torch.device(
        "cuda:0" if (torch.cuda.is_available() and ngpu > 0) else "cpu"
    )

    def objective(trial):
        print(f"Cuda is available: {torch.cuda.is_available()}")

        ########################
        # Create the generator #
        netG = Generator.from_trial(trial).to(device)

        # Handle multi-GPU if desired
        if (device.type == "cuda") and (ngpu > 1):
            netG = nn.DataParallel(netG, list(range(ngpu)))

        # Apply the ``weights_init`` function to randomly initialize all
        # weights to ``mean=0``, ``stdev=0.02``.
        netG.apply(weights_init)

        ############################
        # Create the Discriminator #
        netD = Discriminator.from_trial(trial).to(device)

        # Handle multi-GPU if desired
        if (device.type == "cuda") and (ngpu > 1):
            netD = nn.DataParallel(netD, list(range(ngpu)))

        # Apply the ``weights_init`` function to randomly initialize all
        # weights like this: ``to mean=0, stdev=0.2``.
        netD.apply(weights_init)

        ########################################
        # Remaining crierion, optimizers, etc. #
        # Initialize the ``BCELoss`` function
        criterion = nn.BCELoss()

        # Create batch of latent vectors that we will use to visualize
        #  the progression of the generator
        fixed_noise = torch.randn(64, nz, 1, 1, device=device)

        # Establish convention for real and fake labels during training
        real_label = 1.0
        fake_label = 0.0

        # Learning rate for optimizers
        lr = 0.00001

        # Setup Adam optimizers for both G and D
        optimizerD = optim.Adam(netD.parameters(), lr=lr, betas=(beta1, 0.999))
        optimizerG = optim.Adam(netG.parameters(), lr=lr, betas=(beta1, 0.999))

        #################
        # Training Loop #

        # Lists to keep track of progress
        img_list = []
        G_losses = []
        D_losses = []
        iters = 0

        print("Starting Training Loop...")
        # For each epoch
        for epoch in range(num_epochs):
            # For each batch in the dataloader
            for i, data in enumerate(dataloader, 0):
                ############################
                # (1) Update D network: maximize log(D(x)) + log(1 - D(G(z)))
                #############################

                # Train with all-real batch #
                netD.zero_grad()
                # Format batch
                real_cpu = data[0].to(device)
                b_size = real_cpu.size(0)
                label = torch.full(
                    (b_size,), real_label, dtype=torch.float, device=device
                )
                # Forward pass real batch through D
                output = netD(real_cpu).view(-1)
                # Calculate loss on all-real batch
                errD_real = criterion(output, label)
                # Calculate gradients for D in backward pass
                errD_real.backward()
                D_x = output.mean().item()

                # Train with all-fake batch
                # Generate batch of latent vectors
                noise = torch.randn(b_size, nz, 1, 1, device=device)
                # Generate fake image batch with G
                fake = netG(noise)
                label.fill_(fake_label)
                # Classify all fake batch with D
                output = netD(fake.detach()).view(-1)
                # Calculate D's loss on the all-fake batch
                errD_fake = criterion(output, label)
                # Calculate the gradients for this batch, accumulated (summed)
                # with previous gradients
                errD_fake.backward()
                D_G_z1 = output.mean().item()
                # Compute error of D as sum over the fake and the real batches
                errD = errD_real + errD_fake
                # Update D
                optimizerD.step()

                ############################
                # (2) Update G network: maximize log(D(G(z)))
                ###########################
                netG.zero_grad()
                # fake labels are real for generator cost
                label.fill_(real_label)
                # Since we just updated D, perform another forward pass
                # of all-fake batch through D
                output = netD(fake).view(-1)
                # Calculate G's loss based on this output
                errG = criterion(output, label)
                # Calculate gradients for G
                errG.backward()
                D_G_z2 = output.mean().item()
                # Update G
                optimizerG.step()

                # Output training stats
                if i % 50 == 0:
                    print(
                        f"[{epoch}/{num_epochs}][{i}/{len(dataloader)}]"
                        f"\tLoss_D: {errD.item():.4f}\tLoss_G: {errG.item():.4f}"
                        f"\tD(x): {D_x:.4f}\tD(G(z)): {D_G_z1:.4f} / {D_G_z2:.4f}"
                    )

                # Save Losses for plotting later
                G_losses.append(errG.item())
                D_losses.append(errD.item())

                # Check how the generator is doing by saving G's output on fixed_noise
                if (iters % 500 == 0) or (
                    (epoch == num_epochs - 1) and (i == len(dataloader) - 1)
                ):
                    with torch.no_grad():
                        fake = netG(fixed_noise).detach().cpu()
                    img_list.append(vutils.make_grid(fake, padding=2, normalize=True))

                iters += 1

            # Report to Optuna
            trial.report(errD.item(), epoch)
            if trial.should_prune():
                raise optuna.exceptions.TrialPruned()
        return errD.item()

    study = optuna.create_study(
        direction="minimize",
        storage=DaskStorage(client=pytorch_optuna_client),
    )
    futures = [
        pytorch_optuna_client.submit(study.optimize, objective, n_trials=1, pure=False)
        for _ in range(50)
    ]

    _ = wait(futures)

    breakpoint()


class FakeImageDataset(torch.utils.data.Dataset):
    """
    We create a fake image dataset. This ought to be replaced by
    your actual dataset or pytorch's example datasets. We do this here
    to focus more on computation than actual convergence.
    """

    def __init__(self, count=1_000):
        self.labels = np.random.randint(0, 5, size=count)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        label = self.labels[idx]
        torch.random.seed = label
        img = torch.rand(3, image_size, image_size)
        return img, label


def weights_init(m):
    classname = m.__class__.__name__
    if classname.find("Conv") != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find("BatchNorm") != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


class Generator(nn.Module):
    def __init__(self, ngpu, activation=nn.Sigmoid()):
        super(Generator, self).__init__()
        self.ngpu = ngpu
        self.main = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d(nz, ngf * 8, 4, 1, 0, bias=False),
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
            nn.ConvTranspose2d(ngf, nc, 4, 2, 1, bias=False),
            activation
            # state size. ``(nc) x 64 x 64``
        )

    def forward(self, input):
        return self.main(input)

    @classmethod
    def from_trial(cls, trial):
        activations = [nn.Sigmoid, nn.Tanh]
        idx = trial.suggest_categorical(
            "generator_activation", list(range(len(activations)))
        )
        activation = activations[idx]()
        return cls(ngpu, activation)


class Discriminator(nn.Module):
    def __init__(self, ngpu, leaky_relu_slope=0.2, activation=nn.Sigmoid()):
        super(Discriminator, self).__init__()
        self.ngpu = ngpu
        self.main = nn.Sequential(
            # input is ``(nc) x 64 x 64``
            nn.Conv2d(nc, ndf, 4, 2, 1, bias=False),
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
            activation,
        )

    def forward(self, input):
        return self.main(input)

    @classmethod
    def from_trial(cls, trial):
        activations = [nn.Sigmoid, nn.Tanh]
        idx = trial.suggest_categorical(
            "descriminator_activation", list(range(len(activations)))
        )
        activation = activations[idx]()

        slopes = np.arange(0.1, 0.4, 0.1)
        idx = trial.suggest_categorical(
            "descriminator_leaky_relu_slope", list(range(len(slopes)))
        )
        leaky_relu_slope = slopes[idx]
        return cls(ngpu, leaky_relu_slope, activation)
