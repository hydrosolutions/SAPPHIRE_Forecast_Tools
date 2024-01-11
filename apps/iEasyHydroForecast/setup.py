from setuptools import setup, find_packages

setup(
    name='iEasyHydroForecast',
    version='0.1',
    description='A package for hydrological forecasting',
    long_description='A package for hydrological forecasting in the SAPPHIRE project. The package is integrated with the SAPPHIRE Forecast Tools and with the iEasyHydro software suite.',
    packages=find_packages(),
    author='Beatrice Marti',
    author_email='marti@hydrosolutions.ch',
    install_requires=[
        'numpy',
        'pandas',
        'datetime',
    ],
    classifiers=['License :: OSI Approved :: MIT License',],
    license="MIT",
)
