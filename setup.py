from setuptools import setup

setup(
    name="hubmapbags",
    version="2024.02",
    description="Generates big data bags for public datasets in HuBMAP",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/hubmapconsortium/hubmapbags",
    author="Ivan Cao-Berg",
    author_email="icaoberg@psc.edu",
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='HuBMAP',
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
    python_requires='>=3.6',
    project_urls={
        'Bug Reports': 'https://github.com/hubmapconsortium/hubmapbags/issues',
        'Source': 'https://github.com/hubmapconsortium/hubmapbags/',
    },
)
