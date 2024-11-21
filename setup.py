from setuptools import setup, find_packages

setup(
    name="hubmapbags",
    version="2024.04",
    description="Generates big data bags for public datasets in HuBMAP",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/hubmapconsortium/py-hubmapbags",
    author="Ivan Cao-Berg",
    author_email="icaoberg@psc.edu",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="HuBMAP, big data, dataset generation",
    packages=find_packages(),  # Automatically discover all packages
    install_requires=[
        "pandas",
        "numpy",
        "tqdm",
        "tabulate",
        "pandarallel",
        "scipy",
        "matplotlib",
        "duckdb",
    ],
    python_requires=">=3.10",
    project_urls={
        "Bug Reports": "https://github.com/hubmapconsortium/py-hubmapbags/issues",
        "Source": "https://github.com/hubmapconsortium/py-hubmapbags/",
        "Documentation": "https://hubmapbags.readthedocs.io/",  # Optional, if you have docs
    },
    include_package_data=True,  # Include data files as defined in MANIFEST.in
    zip_safe=False,  # If your package can't be reliably used in zipped form, set to False
)
