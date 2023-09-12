from setuptools import setup

setup(
    name="hubmapbags",
    version="2023.4",
    description="Generates HuBMAP BDBags compatible with CFDE",
    url="https://github.com/hubmapconsortium/hubmapbags",
    author="Ivan Cao-Berg",
    author_email="icaoberg@psc.edu",
    packages=["hubmapbags"],
    install_requires=[
        "pandas",
        "numpy",
        "tqdm",
        "tabulate",
        "pandarallel",
        "scipy",
        "matplotlib",
    ],
)
