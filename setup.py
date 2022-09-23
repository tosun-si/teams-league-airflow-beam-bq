from setuptools import find_packages, setup

setup(
    name="team_league_app",
    version="0.0.1",
    install_requires=[
        'dacite==1.6.0',
        'dependency-injector==4.38.0',
        'toolz==0.12.0'
    ],
    packages=find_packages(),
)
